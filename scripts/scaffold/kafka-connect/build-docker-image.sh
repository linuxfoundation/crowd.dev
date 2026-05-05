#!/usr/bin/env bash


if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
	set -euo pipefail
fi

TAG="sjc.ocir.io/axbydjxa5zuh/kafka-connect:$(date +%s)"
readonly TAG

function download_kafka_connect_http() {
	local base_dir="${1:-.}"
	mkdir -p ${base_dir}/tmp

	if [[ (
        ! -d "${base_dir}/tmp/kafka-connect-http" ||
        -z "$(ls -A "${base_dir}/tmp/kafka-connect-http")" ||
        ! -d "${base_dir}/tmp/kafka-connect-http/kafka-connect-http-8.1.28" ||
        -z "$(ls -A "${base_dir}/tmp/kafka-connect-http/kafka-connect-http-8.1.28")") ]] \
        ; then
		
		echo "Downloading kafka-connect-http"
		wget -q "https://github.com/lensesio/stream-reactor/releases/download/8.1.28/kafka-connect-http-8.1.28.zip" -O $base_dir/tmp/kafka-connect-http.zip
		unzip $base_dir/tmp/kafka-connect-http.zip -d "${base_dir}/tmp/kafka-connect-http"
		rm $base_dir/tmp/kafka-connect-http.zip

	else
		echo "kafka-connect-http already downloaded"
	fi

	# Ensure parent directory exists
	mkdir -p "${base_dir}/tmp/custom-plugins"

	if [[ ! -d "${base_dir}/tmp/custom-plugins/tinybird-append-schema-SMT" ||
        -z "$(ls -A "${base_dir}/tmp/custom-plugins/tinybird-append-schema-SMT" 2>/dev/null)" ]]; then

    	echo "Downloading tinybird-append-schema-SMT"
		wget -q "https://github.com/linuxfoundation/tinybird-append-schema-smt/releases/download/1.0.8/tinybird-append-schema-SMT-1.0.8.zip" -O "${base_dir}/tmp/custom-plugins/tinybird-append-schema-SMT.zip"
    	unzip -q "${base_dir}/tmp/custom-plugins/tinybird-append-schema-SMT.zip" -d "${base_dir}/tmp/custom-plugins/tinybird-append-schema-SMT"
    	rm "${base_dir}/tmp/custom-plugins/tinybird-append-schema-SMT.zip"

	else
		echo "tinybird-append-schema-SMT already downloaded"
	fi
}



function download_dependencies() {
	download_kafka_connect_http
}

function main() {
	docker build -t "${TAG}" .

	echo "----------------------------------------"
	echo "Image built with tag: ${TAG}"
	echo "----------------------------------------"
	echo -n "Type 'y' and press Enter to push the image to the registry. Ctrl+C to cancel: "
	read -r PUSH
	if [ "${PUSH}" = "y" ]; then
		echo "Pushing image to the registry..."
		echo "----------------------------------------"
		docker push "${TAG}"
	else
		echo "Skipping push"
	fi
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  download_dependencies
  main
fi