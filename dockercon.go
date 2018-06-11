package main

import (
	"context"
	"io"
	"os"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
)

func run() error {
	client, err := containerd.New(address)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := namespaces.WithNamespace(context.Background(), "dockercon")

	// pull youtube-dl and ffmpeg images
	image, err := client.Pull(ctx, "docker.io/wernight/youtube-dl:latest", containerd.WithPullUnpack)
	if err != nil {
		return err
	}
	// create youtube-dl container with networking
	youtube, err := client.NewContainer(
		ctx,
		"youtube",
		containerd.WithNewSpec(
			oci.WithImageConfig(image),
			oci.WithHostNamespace(specs.NetworkNamespace), oci.WithHostHostsFile, oci.WithHostResolvconf,
			oci.WithProcessArgs("youtube-dl", "-o", "-", "https://www.youtube.com/watch?v=evEuft7Jqjs"),
		),
		containerd.WithNewSnapshot("youtube", image),
	)
	if err != nil {
		return err
	}
	defer youtube.Delete(ctx, containerd.WithSnapshotCleanup)

	// create ffmpeg container without networking
	ffmpeg, err := client.NewContainer(
		ctx,
		"ffmpeg",
		containerd.WithNewSpec(
			oci.WithImageConfig(image),
			oci.WithProcessArgs("ffmpeg", "-i", "pipe:.mp4", "-f", "mp3", "-b:a", "192K", "-vn", "-"),
		),
		containerd.WithNewSnapshot("ffmpeg", image),
	)
	if err != nil {
		return err
	}
	defer ffmpeg.Delete(ctx, containerd.WithSnapshotCleanup)

	pipeline, err := newPipeLine(ctx)
	if err != nil {
		return err
	}
	var tasks []containerd.Task
	// create tasks for the containers with the pipeline
	dl, err := youtube.NewTask(ctx, pipeline.Left)
	if err != nil {
		return err
	}
	defer dl.Delete(ctx)
	tasks = append(tasks, dl)

	enc, err := ffmpeg.NewTask(ctx, pipeline.Right)
	if err != nil {
		return err
	}
	defer enc.Delete(ctx)

	tasks = append(tasks, enc)
	// get our mp3 output, our only io.Copy!
	go io.Copy(os.Stdout, pipeline.right.Stdout)

	// debug because ffmpeg is hard
	debug(pipeline)

	// wait for tasks before starting
	encoderW, err := enc.Wait(ctx)
	if err != nil {
		return err
	}
	dlW, err := dl.Wait(ctx)
	if err != nil {
		return err
	}
	// start the tasks
	for _, t := range tasks {
		if err := t.Start(ctx); err != nil {
			return err
		}
	}
	<-dlW
	pipeline.left.Close()
	if err := enc.CloseIO(ctx, containerd.WithStdinCloser); err != nil {
		logrus.WithError(err).Error("close encoder STDIN")
	}

	<-encoderW
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
	// magic part
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

const (
	address = "/run/containerd/containerd.sock"
	// youtube dl already has ffmpeg so we can use that for this demo
)

func main() {
	if err := run(); err != nil {
		logrus.Fatal(err)
	}
}

func debug(pipeline *Pipeline) {
	go io.Copy(os.Stderr, pipeline.right.Stderr)
	go io.Copy(os.Stderr, pipeline.left.Stderr)
}
