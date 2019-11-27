#!/usr/bin/env bash

./gradlew build
scala build/libs/ex-1.0-SNAPSHOT.jar "$@"
