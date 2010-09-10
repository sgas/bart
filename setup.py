from distutils.core import setup
from distutils.command.install_data import install_data

import os

from bart import __version__



class InstallBart(install_data):
    # this class is used to filter out data files which should not be overwritten

    def finalize_options(self):
        install_data.finalize_options(self)

        ETC_BART = '/etc/bart'
        if self.root is not None:
            ETC_BART = os.path.join(self.root, ETC_BART[1:])

        if not os.path.exists(ETC_BART):
            os.makedirs(ETC_BART)

        bart_conf = os.path.join(ETC_BART, 'bart.conf')
        bart_umap = os.path.join(ETC_BART, 'usermap')
        bart_vmap = os.path.join(ETC_BART, 'vomap')

        if os.path.exists(bart_conf):
            print "Skipping installation of bart.conf (already exists)"
            self.data_files.remove( ('/etc/bart', ['datafiles/etc/bart.conf']) )

        if os.path.exists(bart_umap):
            print "Skipping installation of usermap (already exists)"
            self.data_files.remove( ('/etc/bart', ['datafiles/etc/usermap']) )

        if os.path.exists(bart_vmap):
            print "Skipping installation of vomap (already exists)"
            self.data_files.remove( ('/etc/bart', ['datafiles/etc/vomap']) )



cmdclasses = {'install_data': InstallBart} 


setup(name='sgas-bart',
      version=__version__,
      description='SGAS Batch system Reporting Tool',
      author='Henrik Thostrup Jensen',
      author_email='htj@ndgf.org',
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

