from setuptools import setup

setup(
    name="mp3tool",
    version="0.3.2",
    packages=["mp3tool"],
    install_requires=["mutagen", "clint"],
    entry_points={
        "console_scripts": [
            "mp3tool = mp3tool.tool:main",
            ],
        },
    )
