from setuptools import setup

execfile("mp3tool/version.py")

setup(
    name="mp3tool",
    version=__version__,
    packages=["mp3tool"],
    install_requires=["mutagen", "clint"],
    entry_points={
        "console_scripts": [
            "mp3tool = mp3tool.tool:main",
            ],
        },
    )
