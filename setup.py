from setuptools import setup

with open('README.md') as f:
    long_description = f.read()


setup(
    name='pyxqueue',
    version='0.0.7',
    description='a multi-processes task queue using redis streams.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/FuGangqiang/pyxqueue',
    author='FuGangqiang',
    author_email='fu_gangqiang@qq.com',
    keywords='redis stream queue',
    license='MIT',

    py_modules=['pyxqueue'],
    install_requires=['redis>=3.0.0'],

    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Environment :: Web Environment',
        'License :: OSI Approved :: MIT License',
    ],
)
