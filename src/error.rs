use std::{
    error::Error,
    ffi::c_int,
    fmt::{Display, Formatter},
    io,
};

#[derive(PartialEq, Eq, Debug)]
pub struct FuseError(c_int);

impl FuseError {
    pub const OPERATION_NOT_PERMITTED: Self = FuseError(libc::EPERM);
    pub const NO_EXIST: Self = FuseError(libc::ENOENT);
    pub const IO: Self = FuseError(libc::EIO);
    pub const NO_SUCH_DEVICE_OR_ADDRESS: Self = FuseError(libc::ENXIO);
    pub const EXEC_FORMAT_ERROR: Self = FuseError(libc::ENOEXEC);
    pub const INVALID_DESCRIPTOR: Self = FuseError(libc::EBADF);
    pub const PERMISSION_DENIED: Self = FuseError(libc::EACCES);
    pub const BAD_ADDRESS: Self = FuseError(libc::EFAULT);
    pub const FILE_EXISTS: Self = FuseError(libc::EEXIST);
    pub const NO_SUCH_DEVICE: Self = FuseError(libc::ENODEV);
    pub const NOT_DIRECTORY: Self = FuseError(libc::ENOTDIR);
    pub const IS_DIRECTORY: Self = FuseError(libc::EISDIR);
    pub const INVALID_ARGUMENT: Self = FuseError(libc::EINVAL);
    pub const FILE_TOO_LARGE: Self = FuseError(libc::EFBIG);
    pub const ILLEGAL_SEEK: Self = FuseError(libc::ESPIPE);
    pub const READ_ONLY_FILE_SYSTEM: Self = FuseError(libc::EROFS);
    pub const DIRECTORY_NOT_EMPTY: Self = FuseError(libc::ENOTEMPTY);

    pub fn last() -> Self {
        let error = io::Error::last_os_error();
        Self(error.raw_os_error().unwrap())
    }
}

impl AsRef<str> for FuseError {
    fn as_ref(&self) -> &str {
        match *self {
            FuseError::OPERATION_NOT_PERMITTED => "Operation not permitted",
            FuseError::NO_EXIST => "No such file or directory",
            FuseError::IO => "I/O error ",
            FuseError::NO_SUCH_DEVICE_OR_ADDRESS => "No such device or address",
            FuseError::EXEC_FORMAT_ERROR => "Exec format error",
            FuseError::INVALID_DESCRIPTOR => "Bad file descriptor",
            FuseError::PERMISSION_DENIED => "Permission denied",
            FuseError::BAD_ADDRESS => "Bad address",
            FuseError::FILE_EXISTS => "File exists",
            FuseError::NO_SUCH_DEVICE => "No such device",
            FuseError::NOT_DIRECTORY => "Not a directory",
            FuseError::IS_DIRECTORY => "Is a directory",
            FuseError::INVALID_ARGUMENT => "Invalid argument",
            FuseError::FILE_TOO_LARGE => "File too large",
            FuseError::ILLEGAL_SEEK => "Illegal seek",
            FuseError::READ_ONLY_FILE_SYSTEM => "Read-only file system",
            FuseError::DIRECTORY_NOT_EMPTY => "Directory is not empty",
            _ => "UNKNOWN",
        }
    }
}

impl Display for FuseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl From<c_int> for FuseError {
    fn from(value: c_int) -> Self {
        FuseError(value)
    }
}

impl From<FuseError> for c_int {
    fn from(value: FuseError) -> Self {
        value.0
    }
}

impl Error for FuseError {}
