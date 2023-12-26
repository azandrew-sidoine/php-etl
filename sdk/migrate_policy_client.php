<?php

require __DIR__ . '/vendor/autoload.php';


function get_employeurs_table_records(\PDO $pdo)
{
    $sql = "SELECT * FROM employeurs";

    return db_select($pdo, $sql, [], true, \PDO::FETCH_ASSOC);
}


function get_employeurs_table_records_count(\PDO $pdo)
{
    $sql = "SELECT COUNT(*)
            FROM employeurs";

    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);

    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetchColumn(0);
}


function get_assure_records(\PDO $pdo, string $employeur)
{
    $sql = "SELECT *
            FROM assures
            WHERE numero_employeur_actuel=? AND etat_assure=?";

    return db_select(
        $pdo,
        $sql,
        [[1, $employeur, \PDO::PARAM_STR], [2, 1, \PDO::PARAM_INT]],
        true,
        \PDO::FETCH_ASSOC
    );
}

/**
 * 
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


function registrant_exists(\PDO $pdo, string $ssn = null)
{
    $sql = "SELECT COUNT(DISTINCT sin)
            FROM ass_registrants
            WHERE sin=?";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);
    $stmt->bindParam(1, $ssn, \PDO::PARAM_STR);
    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetchColumn(0) !== 0;
}

function select_registrant(\PDO $pdo, string $ssn = null)
{
    $sql = "SELECT *
            FROM ass_registrants
            WHERE sin=?";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);
    $stmt->bindParam(1, $ssn, \PDO::PARAM_STR);
    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetch(\PDO::FETCH_ASSOC);
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
        'policy_number' => $record['numero_assure'],
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
 * Insert record on each iteration
 * 
 * @param mixed $record 
 * @param array $options 
 * @param array $policy_holders 
 * @param array $registrants 
 * @param PDO|null $dstPdo 
 * @param PDO|null $pdo 
 * @return void 
 * @throws PDOException 
 * @throws RandomException 
 * @throws Exception 
 */
function insert_records($record, array $options, array &$policy_holders, array &$registrants, \PDO $dstPdo = null, \PDO $pdo = null)
{
    // 
    $registrant_exists = registrant_exists(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $dstPdo), $record['numero_employeur']);

    if ($registrant_exists) {
        printf("Employeur already exists in destination database, dropping from execution context!\n");
        return;
    }

    // Insert registrant/record record
    db_insert(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $dstPdo), 'ass_registrants', [
        'id' => $record['numero_employeur'],
        'registrant_type_id' => 1,
        'name' => $record['raison_sociale'],
        'sin' => $record['numero_employeur'],
    ]);

    // Select the created registrant
    // $result = select_registrant(db_connect(function () use ($options) {
    //     return create_dst_connection($options);
    // }, $dstPdo), $record['numero_employeur']);

    // if (!$result) {
    //     printf("Error while inserting record for employeur [%s]\n", strval($record['numero_employeur']));
    //     return;
    // }

    // Add registrant to cache
    $registrants[$record['numero_employeur']] = $record['numero_employeur'];

    // Insert registrant contact
    db_insert(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $dstPdo), 'ass_registrant_contacts', [
        'id' => str_uuid(),
        'registrant_id' => $record['numero_employeur'],
        'phone_number' => str_after('TEL', $record['address'] ?? ''),
        'email' => null,
        'address' => $record['adresse'],
        'po_box' => str_before('TEL', $record['address'] ?? ''),
    ]);

    // Query of assures records
    $assures = get_assure_records($pdo, $record['numero_employeur']);

    // For each assure record, insert the assure and it carrier
    foreach ($assures as $assure) {
        $policy_holder_id = insert_policy_holder($dstPdo, $assure, $options, $policy_holders);
        if (!is_string($policy_holder_id)) {
            printf("Unable to insert assure [%s] into database\n", $assure['numero_assure']);
            return;
        }

        // Query assure carrier
        $carrieres = get_assures_carriere(
            db_connect(function () use ($options) {
                return create_src_connection($options);
            }, $pdo),
            $assure['numero_assure']
        );

        foreach ($carrieres as $value) {
            $registrant_id = $registrants[$value['numero_employeur']] ?? null;
            // Case the registrant id value is null, continue to the next iteration
            if (null === $registrant_id) {
                continue;
            }

            db_insert(db_connect(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo), 'ass_registrant_policy_holders', [
                'id' => str_uuid(),
                'registrant_id' => $registrant_id,
                'policy_holder_id' => $policy_holder_id,
                'start_date' => $value['date_entree'],
                'end_date' => $value['date_sortie']
            ]);
        }

        // Insert registrant_policy_holder for numero_employeur_actuel
        if (isset($assure['date_embauche']) && isset($assure['numero_employeur_actuel']) && isset($record['numero_employeur'])) {
            db_insert(db_connect(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo), 'ass_registrant_policy_holders', [
                'id' => str_uuid(),
                'registrant_id' => $record['numero_employeur'],
                'policy_holder_id' => $policy_holder_id,
                'start_date' => $assure['date_embauche'],
                'end_date' => null
            ]);
        }
    }
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

/**
 * Process failed insertion
 * 
 * @param array $records 
 * @param mixed $options 
 * @return void 
 */
function process_failed_records(array $records, $options)
{
    printf("Processing failed records...\n");
    $policy_holders = [];
    $registrants = [];
    foreach ($records as $record) {
        try {
            insert_records(
                $record,
                $options,
                $policy_holders,
                $registrants,
                db_connect(function () use ($options) {
                    return create_dst_connection($options);
                }),
                db_connect(function () use ($options) {
                    return create_src_connection($options);
                })
            );
        } catch (\Throwable $e) {
            print_r($e);
            die();
            printf("[%s] Exceptions: %s", date('Y-m-d H:i:s'), $e->getMessage());
        }
    }
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

    $total = get_employeurs_table_records_count(db_connect(function () use ($options) {
        return create_src_connection($options);
    }, $pdo));

    printf(sprintf("Importing a total of %d employeurs...\n", $total));

    // Select list of data to be inserted
    $values = get_employeurs_table_records(db_connect(function () use ($options) {
        return create_src_connection($options);
    }, $pdo));
    $index = 0;
    $policy_holders = [];
    $registrants = [];
    $failed = [];

    foreach ($values as $record) {
        printf("Inserting record for employeur [%s] \n", $record['numero_employeur']);
        try {
            db_connect(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo)->beginTransaction();
            insert_records(
                $record,
                $options,
                $policy_holders,
                $registrants,
                db_connect(function () use ($options) {
                    return create_dst_connection($options);
                }, $dstPdo),
                db_connect(function () use ($options) {
                    return create_src_connection($options);
                }, $pdo)
            );
            db_connect(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo)->commit();
        } catch (\Throwable $e) {
            $conn = db_connect(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo);
            if ($conn) {
                $conn->rollBack();
            }
            print_r($e);
            die();
            printf(sprintf("Exception (%d): %s\n %s", $index, $e->getMessage(), implode(", ", array_values($record))));
            $failed[] = $record;
        }
    }

    if (count($failed) !== 0) {
        printf("Process failed to import a total of %d record, retrying to insert failed migrations...\n", count($failed));
        process_failed_records($failed, $options);
    }
    printf(sprintf("\nThanks for using the program!\n"));
}


// Start program execution
main(array_slice($argv, 1));
