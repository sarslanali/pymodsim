from pbr.version import VersionInfo

package_name='pymodsim'
info = VersionInfo(package_name)

__version__ = info.version_string()