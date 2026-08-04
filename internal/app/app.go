package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	runtimeconfig "github.com/k3rnL/riquet/internal/config"
	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
	avroformat "github.com/k3rnL/riquet/internal/formats/avro"
	jsonschemaformat "github.com/k3rnL/riquet/internal/formats/jsonschema"
	protobufformat "github.com/k3rnL/riquet/internal/formats/protobuf"
	"github.com/k3rnL/riquet/internal/ha"
	"github.com/k3rnL/riquet/internal/observability"
	"github.com/k3rnL/riquet/internal/security"
	"github.com/k3rnL/riquet/internal/storage"
	boltstore "github.com/k3rnL/riquet/internal/storage/bolt"
	kafkastore "github.com/k3rnL/riquet/internal/storage/kafka"
	"github.com/k3rnL/riquet/internal/transport/confluent"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config contains standalone server configuration. Listener is retained for
// focused PVC tests; production configuration is supplied through Runtime.
type Config struct {
	ListenAddress string
	DataPath      string
	Listener      net.Listener
	Runtime       *runtimeconfig.Config
}

type auxiliary struct {
	server *http.Server
	done   chan error
}

// Run recovers committed state, serves until cancellation, snapshots when
// safe, and closes storage. Both PVC and Kafka HA use this same entry point.
func Run(ctx context.Context, config Config) (runErr error) {
	runtime := config.Runtime
	if runtime != nil {
		config.ListenAddress = runtime.Listeners.Public
		config.DataPath = runtime.Storage.PVC.Path
	}
	if config.ListenAddress == "" {
		config.ListenAddress = ":8081"
	}

	store, kafkaStore, err := openStore(ctx, config)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, store.Close()) }()
	state, err := recoverState(ctx, store)
	if err != nil {
		return fmt.Errorf("recover registry: %w", err)
	}

	avroEngine := avroformat.Engine{}
	protobufEngine := protobufformat.Engine{}
	jsonEngine := jsonschemaformat.Engine{}
	engines := map[domain.SchemaType]formats.Engine{
		domain.SchemaTypeAvro: avroEngine, domain.SchemaTypeProtobuf: protobufEngine, domain.SchemaTypeJSON: jsonEngine,
	}
	checker := compatibilityChecker(ctx, engines)
	machine := domain.NewMachine(state, store, checker)
	api := confluent.NewServer(machine, avroEngine, protobufEngine, jsonEngine)

	var coordinator *kafkastore.Coordinator
	internalHandler := api.Handler()
	var followDone <-chan error
	if kafkaStore != nil {
		coordinator, err = kafkastore.NewCoordinator(kafkaStore, kafkastore.CoordinatorOptions{
			GroupID: runtime.Storage.Kafka.GroupID, Advertisement: runtime.Storage.Kafka.AdvertiseURL,
		})
		if err != nil {
			return err
		}
		forwarded, forwardErr := (ha.Forwarder{
			Authority: coordinator, Token: runtime.Auth.InternalToken,
		}).Handler(api.Handler())
		if forwardErr != nil {
			return forwardErr
		}
		internalHandler = forwarded
		follow := make(chan error, 1)
		followDone = follow
		go func() { follow <- kafkaStore.Follow(ctx, machine.State().Sequence(), machine.ApplyCommitted) }()
		go coordinate(ctx, coordinator, runtime.Storage.Kafka.ReplicaID)
		defer func() {
			lease, _ := coordinator.LocalLease()
			runErr = errors.Join(runErr, coordinator.Release(context.Background(), lease))
		}()
	}

	stateProvider := func() observability.State {
		if kafkaStore != nil {
			status := kafkaStore.Status()
			return observability.State{
				Started: true, Ready: status.Ready, BackendHealthy: status.BackendHealthy,
				Role: status.Role, Epoch: status.Epoch, AppliedPosition: status.AppliedPosition,
				CommittedPosition: status.CommittedPosition, Lag: status.Lag,
			}
		}
		health := store.Health(context.Background())
		return observability.State{
			Started: true, Ready: health.Healthy, BackendHealthy: health.Healthy,
			Role: "single", AppliedPosition: int64(health.LastSequence), CommittedPosition: int64(health.LastSequence), Reason: health.Detail,
		}
	}
	api.SetReadyFunc(func() bool { return stateProvider().Ready })
	var extraMetrics func(io.Writer) error
	if kafkaStore != nil {
		extraMetrics = kafkaStore.WriteMetrics
	}
	metrics := observability.NewHTTPMetrics(extraMetrics)
	publicHandler := internalHandler
	if runtime != nil {
		publicHandler = security.Authenticate(security.AuthConfig{
			Mode: runtime.Auth.Mode, Username: runtime.Auth.Username,
			Password: runtime.Auth.Password, BearerToken: runtime.Auth.BearerToken,
		}, security.ProtectAdministration(runtime.Auth.AdminToken, publicHandler))
	}
	publicHandler = observability.LogMiddleware(nil, metrics.Middleware(publicHandler))

	listener := config.Listener
	if listener == nil {
		listener, err = net.Listen("tcp", config.ListenAddress)
		if err != nil {
			return fmt.Errorf("listen: %w", err)
		}
	}
	defer func() { _ = listener.Close() }()
	httpServer := &http.Server{
		Handler: publicHandler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
	}
	if runtime != nil {
		httpServer.ReadTimeout = runtime.Limits.ReadTimeout
		httpServer.WriteTimeout = runtime.Limits.WriteTimeout
	}

	var auxiliaries []auxiliary
	startAuxiliary := func(address string, handler http.Handler) error {
		if address == "" {
			return nil
		}
		auxListener, listenErr := net.Listen("tcp", address)
		if listenErr != nil {
			return listenErr
		}
		auxServer := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
		done := make(chan error, 1)
		go func() { done <- auxServer.Serve(auxListener) }()
		auxiliaries = append(auxiliaries, auxiliary{server: auxServer, done: done})
		return nil
	}
	if runtime != nil {
		if err := startAuxiliary(runtime.Listeners.Health, observability.HealthHandler(stateProvider)); err != nil {
			return shutdownAuxiliaries(auxiliaries, fmt.Errorf("health listener: %w", err))
		}
		if err := startAuxiliary(runtime.Listeners.Metrics, metrics.Handler()); err != nil {
			return shutdownAuxiliaries(auxiliaries, fmt.Errorf("metrics listener: %w", err))
		}
		if err := startAuxiliary(runtime.Listeners.Internal, internalHandler); err != nil {
			return shutdownAuxiliaries(auxiliaries, fmt.Errorf("internal listener: %w", err))
		}
	}

	serveDone := make(chan error, 1)
	go func() {
		if runtime != nil && runtime.TLS.CertFile != "" {
			serveDone <- httpServer.ServeTLS(listener, runtime.TLS.CertFile, runtime.TLS.KeyFile)
			return
		}
		serveDone <- httpServer.Serve(listener)
	}()
	select {
	case serveErr := <-serveDone:
		if errors.Is(serveErr, http.ErrServerClosed) {
			return nil
		}
		return serveErr
	case followErr := <-followDone:
		if followErr != nil && ctx.Err() == nil {
			return fmt.Errorf("follow Kafka registry state: %w", followErr)
		}
	case <-ctx.Done():
	}

	shutdownTimeout := 15 * time.Second
	if runtime != nil {
		shutdownTimeout = runtime.Limits.ShutdownTimeout
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := httpServer.Shutdown(shutdownCtx)
	for _, auxiliary := range auxiliaries {
		shutdownErr = errors.Join(shutdownErr, auxiliary.server.Shutdown(shutdownCtx))
		if serveErr := <-auxiliary.done; !errors.Is(serveErr, http.ErrServerClosed) {
			shutdownErr = errors.Join(shutdownErr, serveErr)
		}
	}
	serveErr := <-serveDone
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	shouldSnapshot := kafkaStore == nil
	if coordinator != nil {
		_, shouldSnapshot = coordinator.LocalLease()
	}
	var snapshotErr error
	if shouldSnapshot {
		snapshotErr = store.SaveSnapshot(shutdownCtx, machine.State().Snapshot())
	}
	return errors.Join(shutdownErr, serveErr, snapshotErr)
}

func openStore(ctx context.Context, config Config) (storage.Store, *kafkastore.Store, error) {
	if config.Runtime != nil && config.Runtime.Storage.Backend == "kafka" {
		value := config.Runtime.Storage.Kafka
		tlsConfig, mechanisms, err := kafkaSecurity(value)
		if err != nil {
			return nil, nil, err
		}
		store, err := kafkastore.Open(ctx, kafkastore.Options{
			Brokers: value.Brokers, Topic: value.Topic, TransactionalID: value.TransactionalID,
			ReplicationFactor: value.ReplicationFactor, AutoCreateTopic: value.AutoCreateTopic,
			TransactionTimeout: value.TransactionTimeout, RequireLeadership: true, MaxReadyLag: value.MaxReadyLag,
			TLS: tlsConfig, SASL: mechanisms,
		})
		return store, store, err
	}
	if config.DataPath == "" {
		config.DataPath = filepath.Join(".riquet", "riquet.db")
	}
	if err := os.MkdirAll(filepath.Dir(config.DataPath), 0o750); err != nil {
		return nil, nil, fmt.Errorf("create data directory: %w", err)
	}
	store, err := boltstore.Open(config.DataPath, boltstore.Options{})
	return store, nil, err
}

func kafkaSecurity(value runtimeconfig.KafkaStorage) (*tls.Config, []sasl.Mechanism, error) {
	var tlsConfig *tls.Config
	if value.TLS.Enabled {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: value.TLS.ServerName}
		if value.TLS.CAFile != "" {
			encoded, err := os.ReadFile(value.TLS.CAFile)
			if err != nil {
				return nil, nil, fmt.Errorf("read Kafka TLS CA: %w", err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(encoded) {
				return nil, nil, errors.New("kafka TLS CA contains no certificates")
			}
			tlsConfig.RootCAs = roots
		}
		if value.TLS.CertFile != "" {
			certificate, err := tls.LoadX509KeyPair(value.TLS.CertFile, value.TLS.KeyFile)
			if err != nil {
				return nil, nil, fmt.Errorf("load Kafka TLS client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{certificate}
		}
	}
	var mechanisms []sasl.Mechanism
	switch value.SASL.Mechanism {
	case "":
	case "plain":
		mechanisms = []sasl.Mechanism{plain.Auth{User: value.SASL.Username, Pass: value.SASL.Password}.AsMechanism()}
	case "scram-sha-256":
		mechanisms = []sasl.Mechanism{scram.Auth{User: value.SASL.Username, Pass: value.SASL.Password}.AsSha256Mechanism()}
	case "scram-sha-512":
		mechanisms = []sasl.Mechanism{scram.Auth{User: value.SASL.Username, Pass: value.SASL.Password}.AsSha512Mechanism()}
	default:
		return nil, nil, fmt.Errorf("unsupported Kafka SASL mechanism %q", value.SASL.Mechanism)
	}
	return tlsConfig, mechanisms, nil
}

func recoverState(ctx context.Context, store storage.Store) (domain.State, error) {
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil {
		return domain.State{}, err
	}
	state := domain.NewState()
	if snapshot != nil {
		state, err = domain.Restore(*snapshot)
		if err != nil {
			return domain.State{}, err
		}
	}
	err = store.Replay(ctx, state.Sequence(), func(envelope domain.Envelope) error {
		var applyErr error
		state, applyErr = state.Apply(envelope)
		return applyErr
	})
	return state, err
}

func compatibilityChecker(ctx context.Context, engines map[domain.SchemaType]formats.Engine) domain.CompatibilityCheck {
	return func(current domain.State, level domain.CompatibilityLevel, candidate domain.Schema, previous []domain.Schema) (bool, []string) {
		engine := engines[candidate.Type]
		if engine == nil {
			return false, []string{"schema format engine is unavailable"}
		}
		resolver := appResolver{state: current}
		parsedCandidate, err := engine.Parse(ctx, formats.ParseRequest{Definition: candidate.Definition, References: candidate.References, Limits: formats.DefaultLimits()}, resolver)
		if err != nil {
			return false, []string{err.Error()}
		}
		parsedPrevious := make([]formats.Parsed, 0, len(previous))
		for _, schema := range previous {
			parsed, parseErr := engine.Parse(ctx, formats.ParseRequest{Definition: schema.Definition, References: schema.References, Limits: formats.DefaultLimits()}, resolver)
			if parseErr != nil {
				return false, []string{parseErr.Error()}
			}
			parsedPrevious = append(parsedPrevious, parsed)
		}
		compatible, messages, err := engine.Compatible(ctx, level, parsedCandidate, parsedPrevious)
		if err != nil {
			return false, []string{err.Error()}
		}
		return compatible, messages
	}
}

func coordinate(ctx context.Context, coordinator *kafkastore.Coordinator, replicaID string) {
	for ctx.Err() == nil {
		lease, err := coordinator.Acquire(ctx, replicaID)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
				continue
			}
		}
		ticker := time.NewTicker(time.Second)
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				if coordinator.Renew(ctx, lease) != nil {
					ticker.Stop()
					goto reacquire
				}
			}
		}
	reacquire:
	}
}

func shutdownAuxiliaries(auxiliaries []auxiliary, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := cause
	for _, auxiliary := range auxiliaries {
		result = errors.Join(result, auxiliary.server.Shutdown(ctx))
	}
	return result
}

type appResolver struct{ state domain.State }

func (r appResolver) Resolve(_ context.Context, reference domain.Reference) (domain.Schema, error) {
	_, schema, ok := r.state.Lookup(reference.Subject, reference.Version, false)
	if !ok {
		return domain.Schema{}, errors.New("referenced schema not found")
	}
	return schema, nil
}
