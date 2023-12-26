<?php

use Random\RandomException;

require __DIR__ . '/vendor/autoload.php';

/**
 * 
 * @param PDO $pdo 
 * @param string|null $name 
 * @return mixed 
 * @throws PDOException 
 */
function get_table_record_count(\PDO $pdo, string $name = null, $status = null)
{
    $values = $name ? explode(',', $name) : [];
    $sql = "SELECT COUNT(DISTINCT a.numero_assure)
            FROM assures as a
            JOIN employeurs as e
            ON a.numero_employeur_actuel = e.numero_employeur";

    // Add registrant name filter to the query
    $conditions = [];
    if (null !== $name) {
        $in  = str_repeat('?,', count($values) - 1) . '?';
        $conditions[] = "e.raison_sociale IN ($in)";
    }

    // Add policy holder status filter to the query
    if (null !== $status) {
        $values[] = $status;
        $conditions[] =  "a.etat_assure=?";
    }

    if (!empty($conditions)) {
        $sql .= (" WHERE ") . (implode(" AND ", $conditions));
    }


    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);

    // Execute the PDO statement
    $stmt->execute($values);

    // Return the result of the query
    return $stmt->fetchColumn(0);
}

/**
 * Get a list of assures matching a given record
 * 
 * @param PDO $pdo 
 * @param string|null $name 
 * @return array|Iterator 
 * @throws PDOException 
 */
function get_assures_records(\PDO $pdo, string $name = null, $status = null)
{
    $values = $name ? explode(',', $name) : [];
    $sql = "SELECT a.*, e.*
            FROM assures as a
            JOIN employeurs as e
            ON a.numero_employeur_actuel = e.numero_employeur";

    $conditions = [];
    if (null !== $name) {
        $in  = str_repeat('?,', count($values) - 1) . '?';
        $conditions[] = "e.raison_sociale IN ($in)";
    }

    // Add policy holder status filter to the query
    if (null !== $status) {
        $values[] = $status;
        $conditions[] =  "a.etat_assure=?";
    }

    if (!empty($conditions)) {
        $sql .= (" WHERE ") . (implode(" AND ", $conditions));
    }

    $index = 1;
    $params =  array_map(function ($value) use (&$index) {
        $result = [$index, $value];
        $index++;
        return $result;
    }, $values);

    return db_select(
        $pdo,
        $sql,
        $params,
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
function fetch_assure_carriere(\PDO $pdo, string $ssn = null)
{
    $sql = "SELECT numero_assure, numero_employeur, date_entree, date_sortie
            FROM carriere_assures
            WHERE numero_assure LIKE :ssn";
    return db_select($pdo, $sql, [['ssn', $ssn, \PDO::PARAM_STR]], true, \PDO::FETCH_ASSOC);
}

function policy_holder_exists(\PDO $pdo, string $ssn = null)
{
    $sql = "SELECT COUNT(DISTINCT sin)
            FROM ass_policy_holders
            WHERE sin LIKE :ssn";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);
    $stmt->bindParam('ssn', $ssn, \PDO::PARAM_STR);
    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetchColumn(0) !== 0;
}


function registrant_exists(\PDO $pdo, string $ssn = null)
{
    $sql = "SELECT COUNT(DISTINCT sin)
            FROM ass_registrants
            WHERE sin LIKE :ssn";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);
    $stmt->bindParam('ssn', $ssn, \PDO::PARAM_STR);
    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetchColumn(0) !== 0;
}

function select_registrant(\PDO $pdo, string $ssn = null)
{
    $sql = "SELECT *
            FROM ass_registrants
            WHERE sin LIKE :ssn";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);
    $stmt->bindParam('ssn', $ssn, \PDO::PARAM_STR);
    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetch(\PDO::FETCH_ASSOC);
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
 * 
 * @param callable $conn_factory 
 * @param PDO|null $connection 
 * @return mixed 
 * @throws RuntimeException 
 */
function resolve_connection(callable $conn_factory, \PDO $connection = null)
{
    if (null !== $connection) {
        return $connection;
    }

    $attempts = 0;

    while ($attempts < 7) {
        // Wait for 5 second to create a connection
        usleep(1000 * 1000 * 5);
        printf("Attempting to reconnect...\n");
        $connection = call_user_func($conn_factory);
        if (null !== $connection) {
            return $connection;
        }
        $attempts++;
    }
    throw new RuntimeException('Too many attempt to create database connection');
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
    try {

        // Start a transaction
        resolve_connection(function () use ($options) {
            return create_dst_connection($options);
        }, $dstPdo)->beginTransaction();
        $person = [
            'id' => str_uuid(),
            'firstname' => $record['prenoms'],
            'lastname' => $record['nom'],
            'sex' => $record['sexe'],
            'birth_date' => $record['date_naissance'],
        ];

        // Insert person
        db_insert(resolve_connection(function () use ($options) {
            return create_dst_connection($options);
        }, $dstPdo), 'ass_persons', $person);
        // printf(sprintf("Inserted person record: %s\n", $index));

        // printf(sprintf("Inserting policy holder record...\n"));
        if (!policy_holder_exists(resolve_connection(function () use ($options) {
            return create_dst_connection($options);
        }, $dstPdo), $record['numero_assure'])) {

            $policy_holder = [
                'id' => str_uuid(),
                'policy_holder_type_id' => $record['type_assure'],
                'person_id' => $person['id'],
                'enrolled_at' => $record['date_immatriculation'] ?? null,
                'sin' => $record['numero_assure'],
                'policy_number' => $record['numero_assure'],
                'status' => $record['etat_assure']
            ];

            // Insert registrant
            db_insert(resolve_connection(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo), 'ass_policy_holders', $policy_holder);
            $policy_holders[$record['numero_assure']] = $policy_holder['id'];

            $contact = [
                'id' => str_uuid(),
                'policy_holder_id' => $policy_holder['id'],
                'phone_number' => $record['tel'] ?? '',
                'email' => $record['email'],
                'po_box' => $record['bp_ville'],
                'address' => $record['rue'] ?? null
            ];
            // Insert policy holder contact
            db_insert(resolve_connection(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo), 'ass_policy_holder_contacts', $contact);

            // printf(sprintf("Inserted policy holder record: %d\n", $index));
        }

        // printf(sprintf("Inserting redistrant record...\n"));
        // Select registrant_type from database
        if (!registrant_exists(resolve_connection(function () use ($options) {
            return create_dst_connection($options);
        }, $dstPdo), $record['numero_employeur'])) {
            $registrant = [
                'registrant_type_id' => 1,
                'name' => $record['raison_sociale'],
                'sin' => $record['numero_employeur'],
            ];

            // Insert registrant
            db_insert(resolve_connection(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo), 'ass_registrants', $registrant);

            if ($result = select_registrant(resolve_connection(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo), $record['numero_employeur'])) {
                $registrants[$record['numero_employeur']] = $result['id'];
                $registrant_contact = [
                    'id' => str_uuid(),
                    'registrant_id' => $result['id'],
                    'phone_number' => str_after('TEL', $record['address'] ?? ''),
                    'email' => null,
                    'address' => $record['adresse'],
                    'po_box' => str_before('TEL', $record['address'] ?? ''),
                ];

                // Insert registrant contact
                db_insert(resolve_connection(function () use ($options) {
                    return create_dst_connection($options);
                }, $dstPdo), 'ass_registrant_contacts', $registrant_contact);
                // printf(sprintf("Inserted registrant record: %d\n", $index));
            } else {
                throw new Exception('Registrant not found!');
            }
        }

        // Case the source pdo instance is null, create a new connection
        $carrieres = fetch_assure_carriere(
            resolve_connection(function () use ($options) {
                return create_src_connection($options);
            }, $pdo),
            $record['numero_assure']
        );
        foreach ($carrieres as $value) {
            $registrant_id = $registrants[$value['numero_employeur']] ?? null;
            $policy_holder_id = $policy_holders[$value['numero_assure']] ?? null;

            if ((null === $registrant_id) || (null === $policy_holder_id)) {
                continue;
            }
            db_insert(resolve_connection(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo), 'ass_registrant_policy_holders', [
                'id' => str_uuid(),
                'registrant_id' => $registrant_id,
                'policy_holder_id' => $policy_holder_id,
                'start_date' => $value['date_entree'],
                'end_date' => $value['date_sortie'],
                // 'income' => null
            ]);
        }

        // Commit the transaction
        resolve_connection(function () use ($options) {
            return create_dst_connection($options);
        }, $dstPdo)->commit();
    } catch (\Throwable $e) {
        // Rollback the started transaction
        $conn = resolve_connection(function () use ($options) {
            return create_dst_connection($options);
        }, $dstPdo);
        if ($conn) {
            $conn->rollBack();
        }

        // Rethrow the exception to allow caller to handle the exception
        throw $e;
    }
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
    $policy_holders = [];
    $registrants = [];
    foreach ($records as $record) {
        try {
            insert_records(
                $record,
                $options,
                $policy_holders,
                $registrants,
                resolve_connection(function () use ($options) {
                    return create_dst_connection($options);
                }),
                resolve_connection(function () use ($options) {
                    return create_src_connection($options);
                })
            );
        } catch (\Throwable $e) {
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
        list($optionsArgs, $argument) = [[], null];
    } else if ('-' === substr(strval($args[0]), 0, 1)) {
        // Case the total list of argument is 1 or the last element starts with - or --, we do not treat the last argument as command argument
        $optionsArgs = array_slice($args, 0, ((count($args) === 1) || ('-' === substr(strval($args[count($args) - 1]), 0, 1)) ? null : count($args) - 1));
        $argument = array_slice($args, count($optionsArgs))[0] ?? null;
    } else {
        $argument = $args[0];
        $optionsArgs = array_slice($args, 1);
    }
    $options = console_get_options($optionsArgs);
    // #endregion Load command line arguments and options

    // Creates a PDO instance
    try {
        $pdo = resolve_connection(function() use ($options) {
            return create_src_connection($options);
        });
        $dstPdo = resolve_connection(function () use ($options) {
            return create_dst_connection($options);
        });
    } catch (\Throwable $e) {
        // Wait for 5 to 15 more seconds and try to reconnect
        usleep(1000 * 1000 * rand(5, 15));
        $pdo = resolve_connection(function() use ($options) {
            return create_src_connection($options);
        });
        $dstPdo = resolve_connection(function () use ($options) {
            return create_dst_connection($options);
        });
    }

    $name = $argument ?? null;

    $total = get_table_record_count(resolve_connection(function() use ($options) {
        return create_src_connection($options);
    }, $pdo), $name, 1);

    printf(sprintf("Importing a total of %d policy holders...\n", $total));

    // // Create progress indicator
    // $progress = console_create_progress('[%bar%] %percent%', '#', ' ', 80, $total, [
    //     'ansi_terminal' => true,
    //     'ansi_clear' => true,
    // ]);

    // Select list of data to be inserted
    $values = get_assures_records(resolve_connection(function() use ($options) {
        return create_src_connection($options);
    }, $pdo), $name, 1);
    $index = 0;
    $policy_holders = [];
    $registrants = [];
    $failed = [];

    foreach ($values as $record) {
        try {
            insert_records(
                $record,
                $options,
                $policy_holders,
                $registrants,
                resolve_connection(function () use ($options) {
                    return create_dst_connection($options);
                }, $dstPdo),
                resolve_connection(function() use ($options) {
                    return create_src_connection($options);
                }, $pdo)
            );
        } catch (\Throwable $e) {
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
