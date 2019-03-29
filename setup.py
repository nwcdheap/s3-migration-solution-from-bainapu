import io
from setuptools import setup
import s3_tools


with io.open("README.md", "rt", encoding="utf8") as f:
    readme = f.read()


static_setup_params = dict(
    name=s3_tools.__prog__,
    version=s3_tools.__version__,
    description='Useful tools for AWS S3.',
    long_description=readme,
    author='wuwentao',
    author_email='wuwentao@patsnap.com',
    url='',
    keywords='AWS, S3, migration',
    python_requires='>=3.5',
    packages=['s3_tools'],
    install_requires=[
        'hsettings'
    ],
    classifiers=[
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        ],
    # Installing as zip files would break due to references to __file__
    zip_safe=False
)


def main():
    """Invoke installation process using setuptools."""
    setup(**static_setup_params)


if __name__ == '__main__':
    main()
