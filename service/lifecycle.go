package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func New(backend Backend, o Options) (*Server, error) {
	if backend == nil {
		return nil, ErrUnauthorized
	}
	o, err := o.checked()
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	s := &Server{backend: backend, options: o, sessions: make(map[*session]struct{}), changed: make(chan struct{}), closed: make(chan struct{}), life: life, cancel: cancel}
	for token, c := range o.Credentials {
		s.credentials = append(s.credentials, authentication{digest: sha256.Sum256([]byte(token)), credential: c})
	}
	return s, nil
}
func (s *Server) notifyLocked() { close(s.changed); s.changed = make(chan struct{}) }

// Listen never removes an existing path or changes permissions of an existing
// directory. The owner supplies a private directory; missing parents are an
// explicit setup error. Managed parent components cannot be symbolic links.
func (s *Server) Listen(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return apiError(api.ErrorClosing)
	}
	if s.listener != nil || s.serving {
		return ErrAlreadyListening
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return apiError(api.ErrorInvalidRequest)
	}
	parent := filepath.Dir(path)
	info, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("%w: socket parent must be a private directory", apiError(api.ErrorInvalidRequest))
	}
	if err = checkSocketOwner(info); err != nil {
		return err
	}
	// Resolve platform aliases such as macOS /tmp before recording ownership;
	// the private parent itself must not be a symbolic link.
	realParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return err
	}
	path = filepath.Join(realParent, filepath.Base(path))
	if _, err := os.Lstat(path); err == nil {
		return ErrSocketExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return err
	}
	ln.SetUnlinkOnClose(false)
	info, statErr := os.Lstat(path)
	if statErr != nil {
		_ = ln.Close()
		return statErr
	}
	if err = os.Chmod(path, 0600); err != nil {
		_ = ln.Close()
		_ = removeOwnedSocket(path, info)
		return err
	}
	s.listener = ln
	s.socketPath = path
	s.socketInfo = info
	return nil
}
func removeOwnedSocket(path string, owned os.FileInfo) error {
	if path == "" || owned == nil {
		return nil
	}
	current, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.Mode()&os.ModeSocket == 0 || !os.SameFile(owned, current) {
		return nil
	}
	return os.Remove(path)
}
func (s *Server) Serve(ctx context.Context) error {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return apiError(api.ErrorClosing)
	}
	if s.listener == nil {
		s.mu.Unlock()
		return errors.New("service is not listening")
	}
	if s.serving {
		s.mu.Unlock()
		return ErrAlreadyListening
	}
	s.serving = true
	ln := s.listener
	s.mu.Unlock()
	stop := context.AfterFunc(ctx, s.beginShutdown)
	defer stop()
	defer s.beginShutdown()
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			stopping := s.stopping
			s.mu.Unlock()
			if stopping {
				return nil
			}
			return err
		}
		s.acceptConn(conn)
	}
}

// acceptConn is shared by the listener and transport-independent tests. Entry
// registration and the stop gate are atomic; shutdown cannot miss a late conn.
func (s *Server) acceptConn(conn net.Conn) bool {
	s.mu.Lock()
	if s.stopping || len(s.sessions) >= s.options.MaxConnections {
		s.mu.Unlock()
		_ = conn.Close()
		return false
	}
	ss := &session{id: rand.Text(), conn: conn, server: s, views: make(map[model.ViewRef]struct{})}
	s.sessions[ss] = struct{}{}
	s.notifyLocked()
	s.mu.Unlock()
	go s.run(ss)
	return true
}
func (s *Server) beginShutdown() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.stopping = true
		ln := s.listener
		sessions := make([]*session, 0, len(s.sessions))
		for ss := range s.sessions {
			sessions = append(sessions, ss)
		}
		s.notifyLocked()
		s.mu.Unlock()
		s.cancel()
		go func() {
			if ln != nil {
				_ = ln.Close()
			}
			for _, ss := range sessions {
				_ = ss.conn.Close()
			}
			for {
				s.mu.Lock()
				if len(s.sessions) == 0 {
					s.mu.Unlock()
					break
				}
				changed := s.changed
				s.mu.Unlock()
				<-changed
			}
			err := removeOwnedSocket(s.socketPath, s.socketInfo)
			s.mu.Lock()
			s.shutdownErr = errors.Join(s.shutdownErr, err)
			s.mu.Unlock()
			close(s.closed)
		}()
	})
}

// Shutdown is sticky and may be retried after a canceled wait. The background
// retirement owner keeps closing sessions even if this particular caller leaves.
func (s *Server) Shutdown(ctx context.Context) error {
	s.beginShutdown()
	select {
	case <-s.closed:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.shutdownErr
	default:
	}
	select {
	case <-s.closed:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
