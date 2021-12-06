package desync

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type ConnectionKey string

var connectionKey = ConnectionKey("connection")

type CredProvider struct {
}

func (p *CredProvider) Retrieve() (credentials.Value, error) {
	return credentials.Value{
		AccessKeyID:     "mainone",
		SecretAccessKey: "thisiskeytrustmedude",
		SessionToken:    "youdontneedtoken",
		SignerType:      credentials.SignatureDefault,
	}, nil
}

func (p *CredProvider) IsExpired() bool {
	return false
}

// func getTestChunk(t *testing.T) (ChunkID, []byte) {
// 	chunkId, err := ChunkIDFromString("9b9a59c17ce4a587ea0a70a4496367e5e48f5812c58e5540d39716568b2f59d0")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	return chunkId, []byte{0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x00, 0x19, 0x00, 0x00, 0x34, 0x32, 0x0a, 0xc0, 0x35, 0x7e, 0x03}
// }

func getTcpS3Server(t *testing.T, ctx context.Context, bucket, prefix, store string) (net.Listener, func() error) {
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	objectGetMatcher := regexp.MustCompile(`^/` + bucket + `/` + prefix + `/([\da-f]+)/([\da-f]+)\.cacnk$`)

	return listener, func() error {
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			r, err := http.ReadRequest(reader)
			if err != nil {
				t.Fatal(err)
			}

			// conn.

			matches := objectGetMatcher.FindStringSubmatch(r.URL.Path)
			if matches == nil {
				// handleS3Get()
			}

			t.Logf("shit is %q | %q | %v", r.Method, r.URL, r.Header)
			conn.Close()
		}
		return nil
	}
}

// Get the S3 mock servier that will give data from chunks and will response invalid HTTP messages
// errorTimesLimit and then response with valid HTTP messages
func getS3Server(t *testing.T, bucket, storageRootPath string, errorWithSend bool) (net.Listener, *http.Server) {
	mux := http.NewServeMux()
	pathSplitter := regexp.MustCompile(`^/` + bucket + `/(.+)$`)
	mux.HandleFunc("/"+bucket+"/", func(w http.ResponseWriter, r *http.Request) {
		matches := pathSplitter.FindStringSubmatch(r.URL.Path)
		if matches == nil {
			w.WriteHeader(400)
			return
		}
		objectPath := path.Join(storageRootPath, matches[1])
		objectStat, err := os.Stat(objectPath)
		if errors.Is(err, os.ErrNotExist) {
			w.WriteHeader(404)
			return
		}
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Add("Last-Modified", time.Now().Format(http.TimeFormat))
		w.Header().Add("Content-Type", "application/octet-stream")
		w.Header().Add("Content-Length", strconv.FormatInt(objectStat.Size(), 10))
		w.WriteHeader(200)

		objectData, err := os.ReadFile(objectPath)
		if err != nil {
			t.Fatal(err)
		}
		if errorWithSend {
			w.Write(objectData[:objectStat.Size()/2])
			conn, ok := r.Context().Value(connectionKey).(net.Conn)
			if !ok {
				t.Fatal("cannot find connection in context")
			}
			conn.Close()
		} else {
			w.Write(objectData)
		}
	})

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	return ln, &http.Server{
		Addr:    ln.Addr().String(),
		Handler: mux,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return context.WithValue(ctx, connectionKey, c)
		},
	}
}

func TestS3Positive(t *testing.T) {
	group, _ := errgroup.WithContext(context.Background())

	// We request object from S3, everything goes fine
	chunkId, err := ChunkIDFromString("f65ae9f509c87b5fbda4229e4f14e3d76bcbb73408e5b400f7b1cdb736e76f46")
	if err != nil {
		t.Fatal(err)
	}
	location := "vertucon-central"
	bucket := "doomsdaydevices"
	ln, server := getS3Server(t, bucket, "cmd/desync/testdata", false)
	group.Go(func() error { return server.Serve(ln) })

	endpoint := url.URL{Scheme: "s3+http", Host: ln.Addr().String(), Path: "/" + bucket + "/blob1.store/"}

	provider := CredProvider{}
	store, err := NewS3Store(&endpoint, credentials.New(&provider), location, StoreOptions{}, minio.BucketLookupAuto)
	if err != nil {
		t.Fatal(err)
	}

	chunk, err := store.GetChunk(chunkId)
	if err != nil {
		t.Fatal(err)
	}

	if chunk.ID() != chunkId {
		t.Errorf("got chunk with id equal to %q, expected %q", chunk.ID(), chunkId)
	}

	server.Shutdown(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Wait(); !errors.Is(err, http.ErrServerClosed) {
		t.Error(err)
	}
}

func TestS3Negative(t *testing.T) {
	group, _ := errgroup.WithContext(context.Background())

	// We request object from S3, everything goes fine
	chunkId, err := ChunkIDFromString("f65ae9f509c87b5fbda4229e4f14e3d76bcbb73408e5b400f7b1cdb736e76f46")
	if err != nil {
		t.Fatal(err)
	}
	location := "vertucon-central"
	bucket := "doomsdaydevices"
	ln, server := getS3Server(t, bucket, "cmd/desync/testdata", true)
	group.Go(func() error { return server.Serve(ln) })

	endpoint := url.URL{Scheme: "s3+http", Host: ln.Addr().String(), Path: "/" + bucket + "/blob1.store/"}

	provider := CredProvider{}
	store, err := NewS3Store(&endpoint, credentials.New(&provider), location, StoreOptions{}, minio.BucketLookupAuto)
	if err != nil {
		t.Fatal(err)
	}

	chunk, err := store.GetChunk(chunkId)
	if err != nil {
		t.Fatal(err)
	}

	if chunk.ID() != chunkId {
		t.Errorf("got chunk with id equal to %q, expected %q", chunk.ID(), chunkId)
	}

	server.Shutdown(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Wait(); !errors.Is(err, http.ErrServerClosed) {
		t.Error(err)
	}
}
