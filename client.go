package cloudfs

import (
	"io"
	"os"
	"path"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"code.google.com/p/go-uuid/uuid"
)

type FileStat struct {
	Inode        uuid.UUID
	Version      uint64
	Size         uint64
	Attrs        []*InodeAttr
	Created      time.Time
	LastModified time.Time
	LastChanged  time.Time
	AdminID      uint32
	QuotaID      uint32
	WriterID     uint32
	ReaderID     uint32
	IsDirectory  bool
	IsFinal      bool
}

type FileSystem struct {
	Interface APIClient
}
type Any struct {
	FS     *FileSystem
	Path   string
	Handle []byte
}
type Dir struct {
	Any
}
type File struct {
	Any
	Pos uint64
}

type StatChangeOption func(*[]*InodeChange, *[]grpc.CallOption)

func CreatedTime(t time.Time) StatChangeOption {
	return func(changes *[]*InodeChange, _ *[]grpc.CallOption) {
		*changes = append(*changes, &InodeChange{
			Action:    InodeChange_SET_CREATED_TIME,
			TimeValue: toInodeTime(t),
		})
	}
}
func LastModifiedTime(t time.Time) StatChangeOption {
	return func(changes *[]*InodeChange, _ *[]grpc.CallOption) {
		*changes = append(*changes, &InodeChange{
			Action:    InodeChange_SET_LAST_MODIFIED_TIME,
			TimeValue: toInodeTime(t),
		})
	}
}
func AdminID(id uint32) StatChangeOption {
	return func(changes *[]*InodeChange, _ *[]grpc.CallOption) {
		*changes = append(*changes, &InodeChange{
			Action:   InodeChange_SET_ADMIN_ID,
			U32Value: id,
		})
	}
}
func QuotaID(id uint32) StatChangeOption {
	return func(changes *[]*InodeChange, _ *[]grpc.CallOption) {
		*changes = append(*changes, &InodeChange{
			Action:   InodeChange_SET_QUOTA_ID,
			U32Value: id,
		})
	}
}
func WriterID(id uint32) StatChangeOption {
	return func(changes *[]*InodeChange, _ *[]grpc.CallOption) {
		*changes = append(*changes, &InodeChange{
			Action:   InodeChange_SET_WRITER_ID,
			U32Value: id,
		})
	}
}
func ReaderID(id uint32) StatChangeOption {
	return func(changes *[]*InodeChange, _ *[]grpc.CallOption) {
		*changes = append(*changes, &InodeChange{
			Action:   InodeChange_SET_READER_ID,
			U32Value: id,
		})
	}
}
func CallOptions(o ...grpc.CallOption) StatChangeOption {
	return func(_ *[]*InodeChange, opts *[]grpc.CallOption) {
		*opts = append(*opts, o...)
	}
}

func (fs *FileSystem) Stat(ctx context.Context, pathname string, opts ...grpc.CallOption) (*FileStat, error) {
	pathname = path.Clean(pathname)
	out, err := fs.Interface.Stat(ctx, &StatRequest{Path: pathname}, opts...)
	if err != nil {
		return nil, &os.PathError{Op: "Stat", Path: pathname, Err: err}
	}
	return &FileStat{
		Inode:        uuid.UUID(out.Inode),
		Version:      out.Version,
		Size:         out.Size,
		Attrs:        out.Attr,
		Created:      fromInodeTime(out.Created),
		LastModified: fromInodeTime(out.LastModified),
		LastChanged:  fromInodeTime(out.LastChanged),
		AdminID:      out.AdminId,
		QuotaID:      out.QuotaId,
		WriterID:     out.WriterId,
		ReaderID:     out.ReaderId,
		IsDirectory:  out.IsDirectory,
		IsFinal:      out.IsFinal,
	}, nil
}

func (fs *FileSystem) ChStat(ctx context.Context, pathname string, opts ...StatChangeOption) error {
	var changes []*InodeChange
	var callOpts []grpc.CallOption
	for _, opt := range opts {
		opt(&changes, &callOpts)
	}

	pathname = path.Clean(pathname)
	_, err := fs.Interface.ChStat(ctx, &ChStatRequest{
		Path:   pathname,
		Change: changes,
	}, callOpts...)
	if err != nil {
		return &os.PathError{Op: "ChStat", Path: pathname, Err: err}
	}
	return nil
}

func (fs *FileSystem) Link(ctx context.Context, oldpath, newpath string, opts ...grpc.CallOption) error {
	oldpath = path.Clean(oldpath)
	newpath = path.Clean(newpath)
	_, err := fs.Interface.Link(ctx, &LinkRequest{OldPath: oldpath, NewPath: newpath}, opts...)
	if err != nil {
		return &os.LinkError{Op: "Link", Old: oldpath, New: newpath, Err: err}
	}
	return nil
}

func (fs *FileSystem) Unlink(ctx context.Context, pathname string, opts ...grpc.CallOption) error {
	pathname = path.Clean(pathname)
	_, err := fs.Interface.Unlink(ctx, &UnlinkRequest{Path: pathname}, opts...)
	if err != nil {
		return &os.PathError{Op: "Unlink", Path: pathname, Err: err}
	}
	return nil
}

func (fs *FileSystem) Rename(ctx context.Context, oldpath, newpath string, opts ...grpc.CallOption) error {
	if err := fs.Link(ctx, oldpath, newpath, opts...); err != nil {
		return err
	}
	if err := fs.Unlink(ctx, oldpath, opts...); err != nil {
		return err
	}
	return nil
}

func (fs *FileSystem) Mkdir(ctx context.Context, pathname string, opts ...StatChangeOption) error {
	if d, err := fs.OpenDir(ctx, pathname, OpenDirMode_DIR_NEW, opts...); err != nil {
		return err
	} else {
		return d.Close(ctx)
	}
}

func (a *Any) Name() string { return a.Path }

func (a *Any) Close(ctx context.Context, opts ...grpc.CallOption) error {
	_, err := a.FS.Interface.Close(ctx, &CloseRequest{Handle: a.Handle}, opts...)
	if err != nil {
		return &os.PathError{Op: "Close", Path: a.Path, Err: err}
	}
	return nil
}

func (a *Any) Stat(ctx context.Context, opts ...grpc.CallOption) (*FileStat, error) {
	return a.FS.Stat(ctx, a.Path, opts...)
}

func (a *Any) ChStat(ctx context.Context, opts ...StatChangeOption) error {
	return a.FS.ChStat(ctx, a.Path, opts...)
}

func (a *Any) LinkTo(ctx context.Context, newpath string, opts ...grpc.CallOption) error {
	return a.FS.Link(ctx, a.Path, newpath, opts...)
}

func (a *Any) RenameTo(ctx context.Context, newpath string, opts ...grpc.CallOption) error {
	return a.FS.Rename(ctx, a.Path, newpath, opts...)
}

func (a *Any) Unlink(ctx context.Context, opts ...grpc.CallOption) error {
	return a.FS.Unlink(ctx, a.Path, opts...)
}

func (fs *FileSystem) OpenDir(ctx context.Context, pathname string, mode OpenDirMode, opts ...StatChangeOption) (*Dir, error) {
	var changes []*InodeChange
	var callOpts []grpc.CallOption
	for _, opt := range opts {
		opt(&changes, &callOpts)
	}

	pathname = path.Clean(pathname)
	out, err := fs.Interface.OpenDir(ctx, &OpenDirRequest{
		Path:   pathname,
		Mode:   mode,
		Change: changes,
	}, callOpts...)
	if err != nil {
		return nil, &os.PathError{Op: "OpenDir", Path: pathname, Err: err}
	}
	return &Dir{Any{fs, pathname, out.Handle}}, nil
}

func (d *Dir) ReadDir(ctx context.Context, cb func(name string, isDir bool), opts ...grpc.CallOption) error {
	var out *ReadDirReply
	stream, err := d.FS.Interface.ReadDir(ctx, &ReadDirRequest{Handle: d.Handle}, opts...)
	for err != nil {
		out, err = stream.Recv()
		if err == nil {
			for _, entry := range out.Entry {
				cb(entry.Filename, entry.IsDirectory)
			}
		}
	}
	if err != io.EOF {
		return &os.PathError{Op: "ReadDir", Path: d.Path, Err: err}
	}
	return nil
}

func (fs *FileSystem) Walk(ctx context.Context, root string, cb func(pathname string, isDir bool), opts ...grpc.CallOption) error {
	var out *WalkReply
	stream, err := fs.Interface.Walk(ctx, &WalkRequest{Root: root}, opts...)
	for err != nil {
		out, err = stream.Recv()
		if err == nil {
			for _, entry := range out.Entry {
				cb(path.Join(out.Dir, entry.Filename), entry.IsDirectory)
			}
		}
	}
	if err != io.EOF {
		return &os.PathError{Op: "Walk", Path: root, Err: err}
	}
	return nil
}

func (fs *FileSystem) OpenFile(ctx context.Context, pathname string, mode OpenFileMode, opts ...StatChangeOption) (*File, error) {
	var changes []*InodeChange
	var callOpts []grpc.CallOption
	for _, opt := range opts {
		opt(&changes, &callOpts)
	}

	pathname = path.Clean(pathname)
	out, err := fs.Interface.OpenFile(ctx, &OpenFileRequest{
		Path:   pathname,
		Mode:   mode,
		Change: changes,
	}, callOpts...)
	if err != nil {
		return nil, &os.PathError{Op: "OpenFile", Path: pathname, Err: err}
	}
	return &File{Any{fs, pathname, out.Handle}, 0}, nil
}

func (fs *FileSystem) Open(ctx context.Context, path string, opts ...StatChangeOption) (*File, error) {
	return fs.OpenFile(ctx, path, OpenFileMode_RD, opts...)
}

func (fs *FileSystem) Create(ctx context.Context, path string, opts ...StatChangeOption) (*File, error) {
	return fs.OpenFile(ctx, path, OpenFileMode_RDWR_NEW, opts...)
}

func (f *File) ReadBig(ctx context.Context, offset uint64, length uint64, cb func([]byte), opts ...grpc.CallOption) error {
	var out *ReadReply
	stream, err := f.FS.Interface.Read(ctx, &ReadRequest{Handle: f.Handle, Offset: offset, Length: length}, opts...)
	var n uint64
	for err != nil {
		out, err = stream.Recv()
		if out != nil {
			cb(out.Data)
			n += uint64(len(out.Data))
		}
	}
	if err == io.EOF {
		if n == length {
			return nil
		}
		return io.EOF
	}
	return &os.PathError{Op: "Read", Path: f.Path, Err: err}
}

func (f *File) ReadAt(ctx context.Context, buf []byte, offset uint64, opts ...grpc.CallOption) (int, error) {
	tmp := make([]byte, 0, len(buf))
	err := f.ReadBig(ctx, offset, uint64(len(buf)), func(data []byte) {
		tmp = append(tmp, data...)
	}, opts...)
	if err == nil || err == io.EOF {
		copy(buf[:len(tmp)], tmp)
		return len(tmp), err
	}
	return 0, err
}

func (f *File) Read(ctx context.Context, buf []byte, opts ...grpc.CallOption) (int, error) {
	n, err := f.ReadAt(ctx, buf, f.Pos, opts...)
	f.Pos += uint64(n)
	return n, err
}

func (f *File) Seek(offset int64, whence int) (uint64, error) {
	var err error
	switch whence {
	case 0:
		if offset >= 0 {
			f.Pos = uint64(offset)
		} else {
			f.Pos = 0
		}
	case 1:
		if offset >= 0 {
			f.Pos += uint64(offset)
		} else if f.Pos >= uint64(-offset) {
			f.Pos -= uint64(-offset)
		} else {
			f.Pos = 0
		}
	case 2:
		err = grpc.Errorf(codes.Unimplemented, "SEEK_END not implemented")
	default:
		err = grpc.Errorf(codes.InvalidArgument, "invalid whence %d", whence)
	}
	if err != nil {
		return f.Pos, &os.PathError{Op: "Seek", Path: f.Path, Err: err}
	}
	return f.Pos, nil
}

func (f *File) Tell() uint64 {
	return f.Pos
}

func (f *File) Append(ctx context.Context, buf []byte, opts ...grpc.CallOption) error {
	_, err := f.FS.Interface.Append(ctx, &AppendRequest{Handle: f.Handle, Data: buf}, opts...)
	if err != nil {
		return &os.PathError{Op: "Append", Path: f.Path, Err: err}
	}
	return nil
}

func (f *File) Truncate(ctx context.Context, offset uint64, opts ...grpc.CallOption) error {
	_, err := f.FS.Interface.Truncate(ctx, &TruncateRequest{Handle: f.Handle, Offset: offset})
	if err != nil {
		return &os.PathError{Op: "Truncate", Path: f.Path, Err: err}
	}
	return nil
}

func (f *File) Finalize(ctx context.Context, opts ...grpc.CallOption) error {
	_, err := f.FS.Interface.Finalize(ctx, &FinalizeRequest{Handle: f.Handle})
	if err != nil {
		return &os.PathError{Op: "Finalize", Path: f.Path, Err: err}
	}
	return nil
}

func fromInodeTime(t *InodeTime) time.Time {
	if t == nil {
		return time.Time{}
	}
	return time.Unix(t.Seconds, int64(t.Nanoseconds))
}

func toInodeTime(t time.Time) *InodeTime {
	if t.IsZero() {
		return nil
	}
	return &InodeTime{Seconds: t.Unix(), Nanoseconds: uint32(t.Nanosecond())}
}
