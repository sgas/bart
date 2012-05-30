import os
from distutils.core import setup
from distutils.command.install import install
from distutils.command.install_data import install_data

from bart import __version__


# nasty global for relocation
RELOCATE = None

class InstallBart(install):

    def finalize_options(self):
        install.finalize_options(self)

        global RELOCATE ; RELOCATE = self.home



class InstallDataBart(install_data):
    # this class is used to filter out data files which should not be overwritten

    def finalize_options(self):
        install_data.finalize_options(self)

        # relocation
        if RELOCATE:
            print 'relocating to %s' % RELOCATE
            for (prefix, files) in reversed(self.data_files):
                if prefix.startswith('/'):
                    new_prefix = os.path.join(RELOCATE, prefix[1:])
                    self.data_files.remove((prefix, files))
                    self.data_files.append((new_prefix, files))

        # check that we don't overwrite /etc files
        for (prefix, files) in reversed(self.data_files):
            if prefix.startswith(os.path.join(RELOCATE or '/', 'etc')):
                for basefile in files:
                    fn = os.path.join(prefix, os.path.basename(basefile))
                    if os.path.exists(fn):
                        print 'Skipping installation of %s (already exists)' % fn
                        files.remove(basefile)
            if not files:
                self.data_files.remove((prefix, []))


cmdclasses = {'install': InstallBart, 'install_data': InstallDataBart} 


setup(name='sgas-bart',
      version=__version__,
      description='SGAS Batch system Reporting Tool',
      author='Henrik Thostrup Jensen / Magnus Jonsson',
      author_email='magnus@hpc2n.umu.se',
      url='http://www.sgas.se/',
      packages=['bart'],
      scripts = ['bart-logger', 'bart-registrant'],
      cmdclass = cmdclasses,

      data_files = [
        ('/etc/bart', ['datafiles/etc/bart.conf']),
        ('/etc/bart', ['datafiles/etc/usermap']),
        ('/etc/bart', ['datafiles/etc/vomap'])
      ]

)

