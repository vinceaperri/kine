package endpoint

import (
	"context"
	"net"
	"os"
	"strings"

	"github.com/vinceaperri/kine/pkg/drivers/generic"
	"github.com/vinceaperri/kine/pkg/drivers/sqlite"
	"github.com/vinceaperri/kine/pkg/server"
	"github.com/vinceaperri/kine/pkg/tls"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const KineSocket = "unix://kine.sock"

type Config struct {
	GRPCServer           *grpc.Server
	Listener             string
	Endpoint             string
	ConnectionPoolConfig generic.ConnectionPoolConfig

	tls.Config
}

type ETCDConfig struct {
	Endpoints   []string
	TLSConfig   tls.Config
}

func Listen(ctx context.Context, config Config) (ETCDConfig, error) {
	backend, err := sqlite.New(ctx, "", config.ConnectionPoolConfig)
	if err != nil {
		return ETCDConfig{}, errors.Wrap(err, "building kine")
	}

	if err := backend.Start(ctx); err != nil {
		return ETCDConfig{}, errors.Wrap(err, "starting kine backend")
	}

	listen := config.Listener
	if listen == "" {
		listen = KineSocket
	}

	b := server.New(backend)
	grpcServer := grpcServer(config)
	b.Register(grpcServer)

	listener, err := createListener(listen)
	if err != nil {
		return ETCDConfig{}, err
	}

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			logrus.Errorf("Kine server shutdown: %v", err)
		}
		<-ctx.Done()
		grpcServer.Stop()
		listener.Close()
	}()

	return ETCDConfig{
		Endpoints:   []string{listen},
		TLSConfig:   tls.Config{},
	}, nil
}

func createListener(listen string) (ret net.Listener, rerr error) {
	network, address := networkAndAddress(listen)

	if network == "unix" {
		if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
			logrus.Warnf("failed to remove socket %s: %v", address, err)
		}
		defer func() {
			if err := os.Chmod(address, 0600); err != nil {
				rerr = err
			}
		}()
	}

	logrus.Infof("Kine listening on %s://%s", network, address)
	return net.Listen(network, address)
}

func grpcServer(config Config) *grpc.Server {
	if config.GRPCServer != nil {
		return config.GRPCServer
	}
	return grpc.NewServer()
}

func networkAndAddress(str string) (string, string) {
	parts := strings.SplitN(str, "://", 2)
	if len(parts) > 1 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}
