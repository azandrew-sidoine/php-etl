<?php

require __DIR__ . '/vendor/autoload.php';

function get_assure_records(\PDO $pdo)
{
    $sql = "SELECT * FROM assures";

    return db_select(
        $pdo,
        $sql,
        [],
        true,
        \PDO::FETCH_ASSOC
    );
}

function policy_holder_exists(\PDO $pdo, string $ssn = null)
{
    $sql = "SELECT COUNT(DISTINCT sin)
            FROM ass_policy_holders
            WHERE sin=?";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);
    $stmt->bindParam(1, $ssn, \PDO::PARAM_STR);
    // Execute the PDO statement
    $stmt->execute();
    // Return the result of the query
    return $stmt->fetchColumn(0) !== 0;
}

function insert_policy_holder(\PDO $pdo, array $record, array $options, &$policy_holders)
{

    // Check if policy holder exists with the ssn
    $policy_holder_exists = policy_holder_exists(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $pdo), $record['numero_assure']);

    if ($policy_holder_exists) {
        return;
    }

    $person_id = str_uuid();
    $person = [
        'id' => $person_id,
        'firstname' => $record['prenoms'],
        'lastname' => $record['nom'],
        'sex' => $record['sexe'],
        'birth_date' => $record['date_naissance'],
        'birth_place' => $record['lieu_naissance'] ?? null,
        'birth_country' => $record['code_pays_nais'] ?? null,
        'nationality' => $record['code_pays_nationalite'] ?? null,
        'marital_status_id' => $record['code_site_matri_actuel'] ?? null,
        'civil_state_id' => $record['code_civilite'] ?? null

    ];

    // Insert person to database
    db_insert(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $pdo), 'ass_persons', $person);


    // Policy holder
    $policy_holder_id = str_uuid();
    $policy_holder = [
        'id' => $policy_holder_id,
        'policy_holder_type_id' => $record['type_assure'],
        'person_id' => $person_id,
        'enrolled_at' => $record['date_immatriculation'] ?? null,
        'sin' => $record['numero_assure'],
        'handicaped' => isset($record['code_etat_handicap']) && strtoupper($record['code_etat_handicap']) === 'O' ? 1 : 0,
        'status' => $record['etat_assure']
    ];
    // Insert policy holder into database
    db_insert(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $pdo), 'ass_policy_holders', $policy_holder);
    $policy_holders[$record['numero_assure']] = $policy_holder_id;

    // Insert policy holder contact
    $contact = [
        'id' => str_uuid(),
        'policy_holder_id' => $policy_holder_id,
        'phone_number' => $record['tel'] ?? '',
        'email' => $record['email'],
        'po_box' => $record['bp_ville'],
        'address' => $record['adresse'] ?? $record['rue'] ?? null
    ];

    // Insert policy holder contact
    db_insert(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $pdo), 'ass_policy_holder_contacts', $contact);

    // Insert in policy holders addresses
    $address = [
        'id' => str_uuid(),
        'policy_holder_id' => $policy_holder_id,
        'country' => $record['code_pays_adr'] ?? null,
        'city' => $record['code_ville'] ?? null,
        'region' => $record['code_region'] ?? null,
        'municipality' => $record['code_commune'] ?? null,
        'prefecture' => $record['code_prefecture'],
        'district' => $record['code_quartier_unique'] ?? null,
        'physical_address' => $record['adresse']
    ];
    db_insert(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $pdo), 'ass_policy_holder_addresses', $address);

    // Insert father's information
    $address = [
        'id' => str_uuid(),
        'policy_holder_id' => $policy_holder_id,
        'firstname' => $record['prenom_pere'] ?? null,
        'lastname' => $record['nom_pere'] ?? null,
        'birth_date' => $record['date_nais_pere'] ?? null,
        'birth_place' => $record['lieu_nais_pere'] ?? null,
        'ancestor_tag' => 'p',
        'ancestor_condition' => $record['etat_pere'] ?? null,
    ];
    db_insert(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $pdo), 'ass_policy_holder_ancestors', $address);

    // Insert mother's information
    $address = [
        'id' => str_uuid(),
        'policy_holder_id' => $policy_holder_id,
        'firstname' => $record['prenom_mere'] ?? null,
        'lastname' => $record['nom_mere'] ?? null,
        'birth_date' => $record['date_nais_mere'] ?? null,
        'birth_place' => $record['lieu_nais_mere'] ?? null,
        'ancestor_tag' => 'm',
        'ancestor_condition' => $record['etat_mere'] ?? null,
    ];
    db_insert(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $pdo), 'ass_policy_holder_ancestors', $address);

    // Return the create policy holder id
    return $policy_holder_id;
}


/**
 * Create a connection to a database server if connection not already created
 * 
 * @param array $options 
 * @return PDO 
 */
function create_dst_connection(array $options)
{
    return create_database_connection($options['dstUser'] ?? "docker", $options['dstPassword'] ?? "homestead", $options['dstHost'] ?? "0.0.0.0", "mysql", $options['dstDb'] ?? "docker", $options['dstPort'] ?? 3306);
}

function create_src_connection(array $options)
{
    return create_database_connection($options['user'] ?? "docker", $options['password'] ?? "homestead", $options['host'] ?? "0.0.0.0", "mysql", $options['db'] ?? "docker", $options['port'] ?? 3306);
}

// Main program
function main(array $args)
{
    // Set the memory limit for the current script execution
    ini_set('memory_limit', '-1');
    set_time_limit(0);

    // Print program description
    $program = 'Program: Migrate data from source database to destination database table - process(' . intval(getmypid()) . ')';
    printf(str_repeat('-', strlen($program)));
    printf(sprintf("\n%s\n", $program));
    printf(sprintf("%s\n", str_repeat('-', strlen($program))));
    // #region Load command line arguments and options
    // In case the list of arguments starts with - or --, the command input configuration is the last parameter, else it's the first parameter
    if (empty($args)) {
        list($optionsArgs) = [[], null];
    } else if ('-' === substr(strval($args[0]), 0, 1)) {
        // Case the total list of argument is 1 or the last element starts with - or --, we do not treat the last argument as command argument
        $optionsArgs = array_slice($args, 0, ((count($args) === 1) || ('-' === substr(strval($args[count($args) - 1]), 0, 1)) ? null : count($args) - 1));
    } else {
        $optionsArgs = array_slice($args, 1);
    }
    $options = console_get_options($optionsArgs);
    // #endregion Load command line arguments and options

    // Creates a PDO instance
    $pdo = db_connect(function () use ($options) {
        return create_src_connection($options);
    });
    $dstPdo = db_connect(function () use ($options) {
        return create_dst_connection($options);
    });
    $assures = get_assure_records($pdo);
    printf(sprintf("Importing a total of %d assures...\n", iterator_count($assures)));

    $policy_holders = [];

    // For each assure record, insert the assure and it carrier
    foreach ($assures as $assure) {
        printf("Inserting record for assures [%s] \n", $assure['numero_assure']);
        $policy_holder_id = insert_policy_holder($dstPdo, $assure, $options, $policy_holders);
        if (!is_string($policy_holder_id)) {
            printf("Unable to insert assure [%s] into database, policy holder already exists\n", $assure['numero_assure']);
            continue;
        }
        printf("Inserted record for assures [%s] \n", $assure['numero_assure']);
    }

    printf(sprintf("\nThanks for using the program!\n"));
}


// Start program execution
main(array_slice($argv, 1));
