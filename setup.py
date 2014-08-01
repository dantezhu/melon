from setuptools import setup, find_packages

setup(
    name="melon",
    version='1.0.38',
    zip_safe=False,
    platforms='any',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    install_requires=['twisted'],
    url="https://github.com/dantezhu/melon",
    license="MIT",
    author="dantezhu",
    author_email="zny2008@gmail.com",
    description="tornado with multiprocessing worker",
)