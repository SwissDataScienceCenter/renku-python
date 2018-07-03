import os
import site
import sys

def main():
    snap = os.environ.get('SNAP')
    if snap is None:
        return

    known_paths = None
    # Add a user site directory if we are running in the context of
    # the Python snap (i.e. not being used through the content interface).
    # We detect this by checking to see if sys.prefix == $SNAP.
    if snap == sys.prefix:
        snap_user_common = os.environ.get('SNAP_USER_COMMON')
        if snap_user_common is not None and site.ENABLE_USER_SITE:
            site.USER_BASE = snap_user_common
            site.USER_SITE = os.path.join(
                site.USER_BASE, 'lib', 'python%d.%d' % sys.version_info[:2])
            if os.path.isdir(site.USER_SITE):
                known_paths = site.addsitedir(site.USER_SITE, known_paths)
    site.addsitepackages(known_paths, [snap])

main()
