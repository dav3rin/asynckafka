# coding=utf-8
import os
import sys

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext

with open(os.path.join(os.path.dirname(__file__), "README.rst")) as f:
    readme = f.read()

version = "0.3.21"
module_name = "asynckafka"
github_username = "jmf-mordis"
language_level = "3"

extensions = [
    "asynckafka.settings",
    "asynckafka.callbacks",
    "asynckafka.utils",
    "asynckafka.consumer.message",
    "asynckafka.consumer.topic_partition",
    "asynckafka.consumer.rd_kafka_consumer",
    "asynckafka.consumer.consumer",
    "asynckafka.producer.rd_kafka_producer",
    "asynckafka.producer.producer",
]

if "--tests" in sys.argv:
    extensions.append("tests.asynckafka_tests")
    sys.argv.remove("--tests")

module_list = [
    Extension(
        extension,
        [extension.replace(".", "/") + ".pyx"],
        libraries=["rdkafka"],
    )
    for extension in extensions
]


class LazyCommandClass(dict):
    def __contains__(self, key):
        return key == "build_ext" or super(LazyCommandClass, self).__contains__(key)

    def __setitem__(self, key, value):
        if key == "build_ext":
            raise AssertionError("build_ext overridden!")
        super(LazyCommandClass, self).__setitem__(key, value)

    def __getitem__(self, key):
        if key != "build_ext":
            return super(LazyCommandClass, self).__getitem__(key)

        class asynckafka_build_ext(build_ext):
            user_options = build_ext.user_options + [
                ("cython-always", None, "run cythonize() even if .c files are present"),
                (
                    "cython-annotate",
                    None,
                    "Produce a colorized HTML version of the Cython source.",
                ),
                ("cython-directives=", None, "Cython compiler directives"),
            ]

            def finalize_options(self):
                from Cython.Build import cythonize

                directives = {
                    "language_level": language_level,
                    "emit_code_comments": True,
                }

                if self.cython_directives:
                    for directive in self.cython_directives.split(","):
                        k, _, v = directive.partition("=")
                        if v.lower() == "false":
                            v = False
                        if v.lower() == "true":
                            v = True
                        directives[k] = v

                self.distribution.ext_modules[:] = cythonize(
                    self.distribution.ext_modules,
                    compiler_directives=directives,
                    annotate=self.cython_annotate,
                    compile_time_env={"CYTHON_GENERATE_PYI": "1"},
                )

                super().finalize_options()

            def build_extensions(self):
                self.compiler.add_library("pthread")
                super().build_extensions()

        return asynckafka_build_ext


setup(
    name=module_name,
    packages=[module_name],
    description="Fast python kafka client for asyncio.",
    long_description=readme,
    url=f"http://github.com/{github_username}/{module_name}",
    license="MIT",
    author="José Melero Fernández",
    author_email="jmelerofernandez@gmail.com",
    platforms=["*nix"],
    version=version,
    download_url=f"https://github.com/{github_username}/{module_name}/archive/{version}.tar.gz",
    cmdclass=LazyCommandClass(),
    setup_requires=[
        "cython>=0.29.21",
        "setuptools>=18.0",
    ],
    install_requires=[],
    ext_modules=module_list,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Framework :: AsyncIO",
    ],
    keywords=["asyncio", "kafka", "cython"],
    test_suite="unittest",
)
