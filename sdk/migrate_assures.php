<?php

// TODO: Update policy holder values in persons, contact and address tables
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

function get_policy_holder(\PDO $pdo, string $ssn = null)
{
    $sql = "SELECT id, sin
            FROM ass_policy_holders
            WHERE sin=?
            LIMIT 1";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);

    // Bind statement parameters
    $stmt->bindParam(1, $ssn, \PDO::PARAM_STR);

    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetch(\PDO::FETCH_ASSOC);
}

/**
 * 
 * @param PDO $pdo 
 * @param string $table 
 * @param string $ssn 
 * @param array $values 
 * @return int 
 * @throws PDOException 
 */
function update_policy_holder(\PDO $pdo, string $table, string $ssn, array $values)
{
    $updates = array_reduce(array_keys($values), function ($carry, $curr) {
        $carry[] = "$curr=?";
        return $carry;
    }, []);
    $sql = "UPDATE $table SET " . implode(", ", $updates) . " WHERE $ssn=?";

    return db_update($pdo, $sql, [...array_values($values), $ssn]);
}

function update_persons(\PDO $pdo, string $table, string $person_id, array $values)
{
    $updates = array_reduce(array_keys($values), function ($carry, $curr) {
        $carry[] = "$curr=?";
        return $carry;
    }, []);
    $sql = "UPDATE $table SET " . implode(", ", $updates) . " WHERE id=?";

    return db_update($pdo, $sql, [...array_values($values), $person_id]);
}

function delete_policy_holder_metadata(\PDO $pdo, string $policy_holder_id)
{
    $sql_s = [
        "DELETE FROM ass_policy_holder_contacts WHERE policy_holder_id=?",
        "DELETE FROM ass_policy_holder_addresses WHERE policy_holder_id=?",
        "DELETE FROM ass_policy_holder_ancestors WHERE policy_holder_id=?"
    ];

    foreach ($sql_s as $sql) {
        $result = db_execute($pdo, $sql, [$policy_holder_id]);
        printf("Deleted a total of %d for policy holder %s\n", $result, $policy_holder_id);
    }
}


/**
 * @param PDO $pdo 
 * @param string|null $ssn 
 * @return array|Iterator 
 * @throws PDOException 
 */
function get_assures_carriere(\PDO $pdo, string $ssn)
{
    $sql = "SELECT numero_assure, numero_employeur, date_entree, date_sortie
            FROM carriere_assures
            WHERE numero_assure=?";
    return db_select($pdo, $sql, [[1, $ssn, \PDO::PARAM_STR]], true, \PDO::FETCH_ASSOC);
}


function insert_policy_holder_metadata(\PDO $pdo, array $record, array $options, $policy_holder_id)
{
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
    $father = [
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
    }, $pdo), 'ass_policy_holder_ancestors', $father);

    // Insert mother's information
    $mother = [
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
    }, $pdo), 'ass_policy_holder_ancestors', $mother);
}

function find_registrant_policy_holder(\PDO $pdo, string $table, $registrant_id, $policy_holder_id)
{
    $sql = "SELECT id
    FROM $table
    WHERE policy_holder_id=? AND registrant_id=?
    LIMIT 1";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);

    // Bind pdo parameters
    $stmt->bindParam(1, $policy_holder_id, \PDO::PARAM_STR);
    $stmt->bindParam(2, $registrant_id, \PDO::PARAM_STR);

    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetch(\PDO::FETCH_ASSOC);
}

function upsert_registrant_policy_holder(\PDO $pdo, array $options, $policy_holder_id, $registrant_id, array $values)
{
    $registrant_policy_holder = find_registrant_policy_holder($pdo, 'ass_registrant_policy_holders', $registrant_id, $policy_holder_id);

    if ($registrant_policy_holder) {
        $updates = array_merge($values, ['registrant_id' => $registrant_id, 'policy_holder_id' => $policy_holder_id]);
        $updates = array_reduce(array_keys($values), function ($carry, $curr) {
            $carry[] = "$curr=?";
            return $carry;
        }, []);
        $sql = "UPDATE ass_registrant_policy_holders SET " . implode(", ", $updates) . " WHERE policy_holder_id=? AND registrant_id=?";
        db_update($pdo, $sql, [...array_values($values), $policy_holder_id, $registrant_id]);
        return;
    }

    db_insert(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $pdo), 'ass_registrant_policy_holders', array_merge($values, ['id' => str_uuid(), 'registrant_id' => $registrant_id, 'policy_holder_id' => $policy_holder_id]));
}

function insert_registrant_policy_holders($record, $policy_holder_id, array $options, \PDO $pdo = null, \PDO $srcPdo = null)
{
    printf("Deleting registrant policy holder for assure: %s\n", $record['numero_assure']);
    $result = db_execute($pdo, "DELETE FROM ass_registrant_policy_holders WHERE policy_holder_id=?", [$policy_holder_id]);
    printf("Deleting a total of %d entries...\n", $result);
    printf("Inserting registrant policy holders records for assure: %s\n", $record['numero_assure']);

    // Query assure carrier
    $carrieres = get_assures_carriere(db_connect(function () use ($options) {
        return create_src_connection($options);
    }, $srcPdo), $record['numero_assure']);

    foreach ($carrieres as $value) {
        db_insert(db_connect(function () use ($options) {
            return create_dst_connection($options);
        }, $pdo), 'ass_registrant_policy_holders', [
            'id' => str_uuid(),
            'start_date' => $value['date_entree'],
            'end_date' => $value['date_sortie'],
            'registrant_id' => $value['numero_employeur'],
            'policy_holder_id' => $policy_holder_id
        ]);
    }

    // Insert registrant_policy_holder for numero_employeur_actuel
    if (isset($record['date_embauche']) && isset($record['numero_employeur_actuel'])) {
        upsert_registrant_policy_holder($pdo, $options, $policy_holder_id, $record['numero_employeur_actuel'], [
            'start_date' => $record['date_embauche'],
            'end_date' => null,
        ]);
    }
}

function insert_policy_holder(\PDO $pdo, \PDO $srcPdo, array $record, array $options, &$policy_holders)
{

    // Check if policy holder exists with the ssn
    $policy_holder = get_policy_holder(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $pdo), $record['numero_assure']);

    // #region updates
    if ($policy_holder && is_array($policy_holder)) {
        printf("Policy holder %s, already exists, updating...\n", strval($record['numero_assure']));

        // We update the policy holder columns values case it already exists
        update_policy_holder(db_connect(function () use ($options) {
            return create_dst_connection($options);
        }, $pdo), 'ass_policy_holders', strval($record['numero_assure']), [
            'policy_holder_type_id' => $record['type_assure'],
            'enrolled_at' => $record['date_immatriculation'] ?? null,
            // 'sin' => $record['numero_assure'],
            'handicaped' => isset($record['code_etat_handicap']) && strtoupper($record['code_etat_handicap']) === 'O' ? 1 : 0,
            'status' => $record['etat_assure']
        ]);

        // Update policy holder ass_persons
        if (isset($policy_holder['person_id'])) {
            update_persons(db_connect(function () use ($options) {
                return create_dst_connection($options);
            }, $pdo), 'ass_persons', $policy_holder['person_id'], [
                'firstname' => $record['prenoms'],
                'lastname' => $record['nom'],
                'sex' => $record['sexe'],
                'birth_date' => $record['date_naissance'],
                'birth_place' => $record['lieu_naissance'] ?? null,
                'birth_country' => $record['code_pays_nais'] ?? null,
                'nationality' => $record['code_pays_nationalite'] ?? null,
                'marital_status_id' => $record['code_site_matri_actuel'] ?? null,
                'civil_state_id' => $record['code_civilite'] ?? null
            ]);
        }
        // TODO: Delete existing contact address and insert new records
        if (isset($policy_holder['id'])) {
            // First we remove any existing policy holder metadata
            printf("Deleting policy holder [%s] metadata...\n", $policy_holder['sin']);
            delete_policy_holder_metadata(db_connect(function () use ($options) {
                return create_dst_connection($options);
            }, $pdo), $policy_holder['id']);

            printf("Inserting new policy holder [%s] metadata...\n", $policy_holder['sin']);
            // The we insert back new values
            insert_policy_holder_metadata(db_connect(function () use ($options) {
                return create_dst_connection($options);
            }, $pdo), $record, $options, $policy_holder['id']);

            // Insert carriere assurés
            insert_registrant_policy_holders($record, $policy_holder['id'], $options, $pdo, $srcPdo);
        }
        return;
    }
    // #endregion updates

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

    // Insert policy holder metadata such as contact, addresses, ancestors
    insert_policy_holder_metadata(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $pdo), $record, $options, $policy_holder_id);


    // Insert carriere assurés
    insert_registrant_policy_holders($record, $policy_holder_id, $options, $pdo, $srcPdo);

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
    $failed_migrations = [];

    // For each assure record, insert the assure and it carrier
    foreach ($assures as $assure) {
        try {
            printf("Inserting record for assure [%s] \n", $assure['numero_assure']);
            $policy_holder_id = insert_policy_holder($dstPdo, $pdo, $assure, $options, $policy_holders);
            if (!is_string($policy_holder_id)) {
                continue;
            }
            printf("Inserted record for assures [%s] \n", $assure['numero_assure']);
        } catch (\PDOException $e) {
            printf("Error while inserting record for assure [%s]: %s \n", $assure['numero_assure'], $e->getMessage());

            $failed_migrations[] = $assure;
            // Case there was a PDO exception when execting migration, we add the failed
            // assure record to the failed task array and recreate connection to the database
            $pdo = db_connect(function () use ($options) {
                return create_src_connection($options);
            });
            $dstPdo = db_connect(function () use ($options) {
                return create_dst_connection($options);
            });
        }
    }

    if (!empty($failed_migrations)) {
        printf("Retrying a total of %d failed migration...\n");
        $pdo = db_connect(function () use ($options) {
            return create_src_connection($options);
        });
        $dstPdo = db_connect(function () use ($options) {
            return create_dst_connection($options);
        });
        foreach ($assures as $assure) {
            try {
                printf("Inserting failed record for assure [%s] \n", $assure['numero_assure']);
                $policy_holder_id = insert_policy_holder($dstPdo, $pdo, $assure, $options, $policy_holders);
                if (!is_string($policy_holder_id)) {
                    continue;
                }
                printf("Inserted record for assures [%s] \n", $assure['numero_assure']);
            } catch (\Throwable $e) {
                // on exception during the failed migrations retry, we just log it to the
                // standard output and continues
                printf("Exception occured during retry: %s\n", $e->getMessage());
            }
        }
    }

    printf(sprintf("\nThanks for using the program!\n"));
}


// Start program execution
main(array_slice($argv, 1));
