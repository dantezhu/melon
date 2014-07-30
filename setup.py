from setuptools import setup

setup(
    name="melon",
    version='1.0.27',
    zip_safe=False,
    platforms='any',
    packages=['melon'],
    install_requires=['twisted'],
    url="https://github.com/dantezhu/melon",
    license="MIT",
    author="dantezhu",
    author_email="zny2008@gmail.com",
    description="tornado with multiprocessing worker",
)