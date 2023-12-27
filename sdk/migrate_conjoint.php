<?php

require __DIR__ . '/vendor/autoload.php';

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

/**
 * @param array $options 
 * @return PDO 
 */
function create_src_connection(array $options)
{
    return create_database_connection($options['user'] ?? "docker", $options['password'] ?? "homestead", $options['host'] ?? "0.0.0.0", "mysql", $options['db'] ?? "docker", $options['port'] ?? 3306);
}

/**
 * Create connection to main application database
 * 
 * @param array $options 
 * @return PDO 
 */
function create_app_connection(array $options)
{
    return create_database_connection($options['appUser'] ?? "docker", $options['appPassword'] ?? "homestead", $options['appHost'] ?? "0.0.0.0", "mysql", $options['appDb'] ?? "docker", $options['appPort'] ?? 3306);
}


function get_conjoints(\PDO $pdo, string $table)
{
    return db_select($pdo, "SELECT * FROM $table", [], true, \PDO::FETCH_ASSOC);
}

function get_assure_conjoints(\PDO $pdo, string $table, string $ssn)
{
    return db_select($pdo, "SELECT * FROM $table WHERE numero_conjoint=?", [[1, $ssn, \PDO::PARAM_STR]], true, \PDO::FETCH_ASSOC);
}


function get_policy_holder(\PDO $pdo, string $table, string $ssn)
{
    $sql = "SELECT id
            FROM $table
            WHERE sin=?
            LIMIT 1";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);

    // Bind pdo parameters
    $stmt->bindParam(1, $ssn, \PDO::PARAM_STR);

    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetch(\PDO::FETCH_ASSOC);
}

function main(array $args)
{
    $program = 'Program: Migrate CNSS conjoints from source to destination database';
    printf(str_repeat('-', strlen($program)));
    printf("\n%s\n", $program);
    printf("%s\n", str_repeat('-', strlen($program)));
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
    $appPdo = db_connect(function () use ($options) {
        return create_app_connection($options);
    });

    $conjoints  = get_conjoints($pdo, 'conjoints');
    $total = iterator_count($conjoints);

    printf("Migrating total of %d conjoints...\n", $total);

    foreach ($conjoints as $conjoint) {
        # code...
        //
        // numero_conjoint
        // etat_conjoint
        // created_at
        // updated_at
        $person_id = str_uuid();
        db_insert(db_connect(function () use ($options) {
            return create_dst_connection($options);
        }, $dstPdo), 'ass_persons', [
            'id' => $person_id,
            'firstname' => $conjoint['prenoms'],
            'lastname' => $conjoint['nom'],
            'sex' => $conjoint['sexe'],
            'birth_date' => $conjoint['date_naissance'],
            'birth_place' => null,
            'birth_country' =>  null,
            'nationality' =>  null,
            'marital_status_id' => null,
            'civil_state_id' => null,
            'created_at' => $conjoint['created_at'],
            'updated_at' => $conjoint['updated_at']
        ]);

        $mariage_bounds = get_assure_conjoints(db_connect(function () use ($options) {
            return create_src_connection($options);
        }), 'assure_conjoints', strval($conjoint['numero_conjoint']));

        printf("Inserting total of %d mariage bound for conjoint: %s\n", iterator_count($mariage_bounds), strval($conjoint['numero_conjoint']));
        foreach ($mariage_bounds as $mariage_bound) {
            $policy_holder = get_policy_holder(db_connect(function () use ($options) {
                return create_app_connection($options);
            }, $dstPdo), 'ass_policy_holders', $mariage_bound['numero_assure']);
            if ($policy_holder && !empty($policy_holder)) {
                db_insert(db_connect(function () use ($options) {
                    return create_dst_connection($options);
                }, $dstPdo), 'ass_mariage_bounds', [
                    'id' => str_uuid(),
                    'policy_holder_id' => strval($policy_holder['id']),
                    'person_id' => $person_id,
                    'policy_number' => strval($conjoint['numero_conjoint']),
                    'bound_at' => $mariage_bound['date_lien'] ?? null,
                    'bound_type_id' => $mariage_bound['type_lien'] ?? null,
                    'spouce_state_id' =>  $mariage_bound['etat_conjoint'] ?? null,
                    'created_at' => $conjoint['created_at'],
                    'updated_at' => $conjoint['updated_at']
                ]);
                continue;
            }
            printf("Policy holder %s does not exists for conjoint: %s\n", strval($mariage_bound['numero_assure']), strval($conjoint['numero_conjoint']));
        }
        
    }
    printf(sprintf("\nThanks for using the program!\n"));
}

main(array_slice($argv, 1));
