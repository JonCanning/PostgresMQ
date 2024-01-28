#!/bin/bash
dotnet tool restore
dotnet paket restore
docker compose up -d
pushd tests || exit
dotnet run
popd || exit
docker compose down
