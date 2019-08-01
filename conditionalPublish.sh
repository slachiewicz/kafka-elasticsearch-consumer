#!/bin/sh

if [ "${TRAVIS_PULL_REQUEST}" = "false" ]; then
    echo '$TRAVIS_PULL_REQUEST is false, running tests and publishing'
    ./gradlew clean test bintrayUpload -x distTar -x distZip -Pbuild=${TRAVIS_BUILD_NUMBER} -Prelease=2.0
else
    echo '$TRAVIS_PULL_REQUEST is not false ($TRAVIS_PULL_REQUEST), running just tests and skipping publish'
    ./gradlew clean test
fi
