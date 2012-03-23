try:
    from setuptools import setup, find_packages, Extension
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages, Extension

setup(
    name='uuss_pb',
    version="",
    #description='',
    #author='',
    #author_email='',
    #url='',
    #install_requires=["Pylons>=0.9.6.2"],
    packages=find_packages(exclude=['ez_setup']),
    #include_package_data=True,
    #test_suite='nose.collector',
    #package_data={'uuss_pb': ['i18n/*/LC_MESSAGES/*.mo']},
    #message_extractors = {'dane': [
    #        ('**.py', 'python', None),
    #        ('templates/**.mako', 'mako', None),
    #        ('public/**', 'ignore', None)]},
    #entry_points="""
    #[paste.app_factory]
    #main = dane.config.middleware:make_app
    #
    #[paste.app_install]
    #main = pylons.util:PylonsInstaller
    #""",
    ext_modules=[Extension('uuss_pb',sources=['cpp/uuss_pb.c','cpp/uuss.pb.cc'], libraries=['protobuf'])]    
)
