CLI_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

function exportEnv() {
    # Key to uppercase
    KEY=$(echo $1 | tr '[:lower:]' '[:upper:]')
    if [[ $2 == "dist" ]]; then
        export $(cat $CLI_HOME/../backend/.env.dist.local | grep $KEY | xargs)
    else
        export $(cat $CLI_HOME/../backend/.env.override.local | grep $KEY | xargs)
    fi
}

function create_nango_integration() {
    # $1 to uppercase
    KEY=$(echo $1 | tr '[:lower:]' '[:upper:]')
    exportEnv "$KEY"

    # We need the client ID, secret and scopes to be set to create the integration
    clientId=$(env | grep -i "^CROWD_${KEY}_CLIENT_ID=" | cut -d'=' -f2-)
    clientSecret=$(env | grep -i "^CROWD_${KEY}_CLIENT_SECRET=" | cut -d'=' -f2-)
    scopes=$(env | grep -i "^CROWD_${KEY}_SCOPES=" | cut -d'=' -f2-)
    if [[ -z $clientId || -z $clientSecret || -z $scopes ]]; then
        printf "\nNot all $1 variables are set. Skipping Nango integration creation.\n"
        printf "The variables needed are: \n- CROWD_${KEY}_CLIENT_ID \n- CROWD_${KEY}_CLIENT_SECRET \n- CROWD_${KEY}_SCOPES"
        return
    else
        printf "\nCreating $1 Integration with client ID: $clientId\n"
        curl    -u "$CROWD_NANGO_SECRET_KEY:" \
                --location 'http://localhost:3003/config' \
                --header 'Content-Type: application/json' \
                --data "{\"provider_config_key\": \"$1\",\"provider\": \"$1\",\"oauth_client_id\": \"$clientId\",\"oauth_client_secret\": \"$clientSecret\",\"oauth_scopes\": \"$scopes\"}"
    fi

}

function create_nango_integrations() {
    exportEnv "NANGO" "dist"
    integrations=$CROWD_NANGO_INTEGRATIONS
    IFS=',' read -ra INTEGRATIONS <<< "$integrations"

    for i in "${INTEGRATIONS[@]}"; do
        create_nango_integration "$i"
    done
}

create_nango_integrations