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

const (
	address        = "/run/containerd/containerd.sock"
	youtubeDLImage = "docker.io/wernight/youtube-dl:latest"
	ffmpegImage    = "docker.io/jrottenberg/ffmpeg:latest"
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

	yImage, err := client.Pull(ctx, youtubeDLImage, containerd.WithPullUnpack)
	if err != nil {
		return err
	}
	ffImage, err := client.Pull(ctx, youtubeDLImage, containerd.WithPullUnpack)
	if err != nil {
		return err
	}

	youtube, err := client.NewContainer(
		ctx,
		"youtube",
		containerd.WithNewSpec(
			oci.WithImageConfig(yImage),
			oci.WithHostNamespace(specs.NetworkNamespace), oci.WithHostHostsFile, oci.WithHostResolvconf,
			oci.WithProcessArgs("youtube-dl", "-o", "-", "https://www.youtube.com/watch?v=evEuft7Jqjs"),
		),
		containerd.WithNewSnapshot("youtube", yImage),
	)
	if err != nil {
		return err
	}
	defer youtube.Delete(ctx, containerd.WithSnapshotCleanup)
	ffmpeg, err := client.NewContainer(
		ctx,
		"ffmpeg",
		containerd.WithNewSpec(
			oci.WithImageConfig(ffImage),
			oci.WithProcessArgs("ffmpeg", "-i", "pipe:.mp4", "-f", "mp3", "-b:a", "192K", "-vn", "-"),
		),
		containerd.WithNewSnapshot("ffmpeg", ffImage),
	)
	if err != nil {
		return err
	}
	defer ffmpeg.Delete(ctx, containerd.WithSnapshotCleanup)

	var tasks []containerd.Task

	pipeline, err := newPipeLine(ctx)
	if err != nil {
		return err
	}

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
	go io.Copy(os.Stdout, pipeline.right.Stdout)
	go io.Copy(os.Stderr, pipeline.right.Stderr)
	go io.Copy(os.Stderr, pipeline.left.Stderr)

	gwait, err := enc.Wait(ctx)
	if err != nil {
		return err
	}
	lswait, err := dl.Wait(ctx)
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
	if err := enc.CloseIO(ctx, containerd.WithStdinCloser); err != nil {
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
