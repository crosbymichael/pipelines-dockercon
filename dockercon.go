package main

import (
	"context"
	"io"
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

	pipeline, err := newPipeLine(ctx)
	if err != nil {
		return err
	}

	ls, err := lsContainer.NewTask(ctx, pipeline.Left)
	if err != nil {
		return err
	}
	defer ls.Delete(ctx)
	tasks = append(tasks, ls)

	grep, err := grepContainer.NewTask(ctx, pipeline.Right)
	if err != nil {
		return err
	}
	defer grep.Delete(ctx)
	tasks = append(tasks, grep)
	go io.Copy(os.Stdout, pipeline.right.Stdout)

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
	pipeline.left.Close()
	if err := grep.CloseIO(ctx, containerd.WithStdinCloser); err != nil {
		logrus.WithError(err).Error("close grepio")
	}

	<-gwait
	return nil
}

func newPipeLine(ctx context.Context) (*Pipeline, error) {
	lf, err := cio.NewFIFOSetInDir("/tmp/fifos", "left", false)
	if err != nil {
		return nil, err
	}
	rf, err := cio.NewFIFOSetInDir("/tmp/fifos", "right", false)
	if err != nil {
		return nil, err
	}
	rf.Stdin = lf.Stdout

	left, err := cio.NewDirectIO(ctx, lf)
	if err != nil {
		return nil, err
	}
	right, err := cio.NewDirectIO(ctx, rf)
	if err != nil {
		return nil, err
	}
	right.Stdin.Close()
	return &Pipeline{
		left:  left,
		right: right,
	}, nil
}

type Pipeline struct {
	left  *cio.DirectIO
	right *cio.DirectIO
}

func (p *Pipeline) Left(_ string) (cio.IO, error) {
	return p.left, nil
}

func (p *Pipeline) Right(_ string) (cio.IO, error) {
	return p.right, nil
}
