
#if defined(__APPLE__) && defined(__MACH__) && !defined(__DARWIN__)
#define DARWIN 1
#endif

#include <unistd.h>

#ifdef DARWIN
#include <fcntl.h>
#endif /* DARWIN */

int os_fdatasync(int fd)
{
#if defined(DARWIN) && defined(F_FULLFSYNC)
    return fcntl(fd, F_FULLFSYNC);
#else
    return fdatasync(fd);
#endif
}
