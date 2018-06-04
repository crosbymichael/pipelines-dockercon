package main

import (
	"context"
	"os"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"
	"github.com/sirupsen/logrus"
)

const (
	address = "/run/containerd/containerd.sock"
	alpine  = "docker.io/library/alpine:latest"
)

func main() {
	if err := run(); err != nil {
		logrus.Fatal(err)
	}
}

func run() error {
	client, err := containerd.New(address)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx := namespaces.WithNamespace(context.Background(), "dockercon")

	image, err := client.Pull(ctx, alpine, containerd.WithPullUnpack)
	if err != nil {
		return err
	}
	lsContainer, err := client.NewContainer(
		ctx,
		"ls",
		containerd.WithNewSpec(
			oci.WithImageConfig(image),
			oci.WithProcessArgs("ls", "-la"),
		),
		containerd.WithNewSnapshot("ls", image),
	)
	if err != nil {
		return err
	}
	defer lsContainer.Delete(ctx, containerd.WithSnapshotCleanup)
	grepContainer, err := client.NewContainer(
		ctx,
		"grep",
		containerd.WithNewSpec(
			oci.WithImageConfig(image),
			oci.WithProcessArgs("grep", "bin"),
		),
		containerd.WithNewSnapshot("grep", image),
	)
	if err != nil {
		return err
	}
	defer grepContainer.Delete(ctx, containerd.WithSnapshotCleanup)

	var tasks []containerd.Task

	lsPipes, err := cio.NewFIFOSetInDir("/tmp/fifos", "ls", false)
	if err != nil {
		return err
	}
	lsIO, err := cio.NewDirectIO(ctx, lsPipes)
	if err != nil {
		return err
	}
	ls, err := lsContainer.NewTask(ctx, func(_ string) (cio.IO, error) {
		return lsIO, nil
	})
	if err != nil {
		return err
	}
	defer ls.Delete(ctx)
	tasks = append(tasks, ls)

	grep, err := grepContainer.NewTask(ctx, cio.NewCreator(cio.WithStreams(lsIO.Stdout, os.Stdout, os.Stderr)))
	if err != nil {
		return err
	}
	defer grep.Delete(ctx)
	tasks = append(tasks, grep)

	gwait, err := grep.Wait(ctx)
	if err != nil {
		return err
	}
	lswait, err := ls.Wait(ctx)
	if err != nil {
		return err
	}
	for _, t := range tasks {
		if err := t.Start(ctx); err != nil {
			return err
		}
	}
	<-lswait
	lsIO.Close()
	if err := grep.CloseIO(ctx, containerd.WithStdinCloser); err != nil {
		logrus.WithError(err).Error("close grepio")
	}

	<-gwait
	return nil
}

func newPipeLine() {
	left * cio.DirectIO
	right * cio.DirectIO
}

type Pipeline struct {
}

func (p *Pipeline) Left(_ string) (cio.IO, error) {

}

func (p *Pipeline) Right(_ string) (cio.IO, error) {

}
