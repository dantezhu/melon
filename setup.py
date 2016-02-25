from setuptools import setup, find_packages

setup(
    name="melon",
    version='1.0.81',
    zip_safe=False,
    platforms='any',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    install_requires=['twisted', 'events', 'setproctitle'],
    url="https://github.com/dantezhu/melon",
    license="MIT",
    author="dantezhu",
    author_email="zny2008@gmail.com",
    description="twisted with multiprocessing worker",
)
