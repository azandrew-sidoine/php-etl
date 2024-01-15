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

function select_policy_holder(\PDO $pdo, string $ssn)
{
    $sql = "SELECT *
            FROM ass_policy_holders
            WHERE sin=?
            LIMIT 1";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);

    $stmt->bindParam(1, $ssn, \PDO::PARAM_STR);

    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetch(\PDO::FETCH_ASSOC);
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

function upsert_registrant_policy_holder(\PDO $pdo, array $options, $registrant_id, $policy_holder_id, array $values)
{
    $registrant_policy_holder = find_registrant_policy_holder($pdo, 'ass_registrant_policy_holders', $registrant_id, $policy_holder_id);
    // Update the registrant policy holder 
    if ($registrant_policy_holder) {
        $updates = array_merge($values, [
            'registrant_id' => $registrant_id,
            'policy_holder_id' => $policy_holder_id
        ]);
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
    }, $pdo), 'ass_registrant_policy_holders', array_merge(
        $values,
        [
            'id' => str_uuid(),
            'registrant_id' => $registrant_id,
            'policy_holder_id' => $policy_holder_id
        ]
    ));
} // insert_registrant_policy_holders

function insert_registrant_policy_holders($registrant_id, array $options, \PDO $dstPdo = null, \PDO $pdo = null)
{
    printf("Inserting registrant policy holders records for employeur: %s\n", $registrant_id);

    // Query of assures records
    $assures = get_assure_records($pdo, $registrant_id);

    // For each assure record, insert the assure and it carrier
    foreach ($assures as $assure) {
        $policy_holder = select_policy_holder($dstPdo, $assure['numero_assure']);
        if (empty($policy_holder)) {
            printf("Unable to find assure [%s] in database\n", $assure['numero_assure']);
            continue;
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
            upsert_registrant_policy_holder($dstPdo, $options, $registrant_id, $policy_holder['id'], [
                'start_date' => $value['date_entree'],
                'end_date' => $value['date_sortie']
            ]);
        }

        // Insert registrant_policy_holder for numero_employeur_actuel
        if (isset($assure['date_embauche']) && isset($assure['numero_employeur_actuel']) && isset($registrant_id)) {
            upsert_registrant_policy_holder($dstPdo, $options, $registrant_id, $policy_holder['id'], [
                'start_date' => $assure['date_embauche'],
                'end_date' => null
            ]);
        }
    }
}

function insert_records($record, array $options, array &$registrants, \PDO $dstPdo = null, \PDO $pdo = null)
{
    // 
    $registrant_exists = registrant_exists(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $dstPdo), $record['numero_employeur']);

    if ($registrant_exists) {
        printf("Employeur already exists in destination database, inserting registrant policy holders and dropping from execution context!\n");
        insert_registrant_policy_holders($record['numero_employeur'], $options, $dstPdo, $pdo);
        return;
    }

    printf("Inserting records for employeur: %s\n", $record['numero_employeur']);

    // Insert registrant/record record
    db_insert(db_connect(function () use ($options) {
        return create_dst_connection($options);
    }, $dstPdo), 'ass_registrants', [
        'id' => $record['numero_employeur'],
        'registrant_type_id' => 1,
        'name' => $record['raison_sociale'],
        'sin' => $record['numero_employeur'],
    ]);

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

    // Insert registrant policy holders
    insert_registrant_policy_holders($record['numero_employeur'], $options, $dstPdo, $pdo);
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
    $registrants = [];
    foreach ($records as $record) {
        try {
            insert_records(
                $record,
                $options,
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
