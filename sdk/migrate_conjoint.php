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

function get_policy_holder(\PDO $pdo, string $table, string $ssn)
{
    $sql = "SELECT id, sin
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

function get_mariage_bound(\PDO $pdo, string $table, string $policy_holder, string $policy)
{
    $sql = "SELECT id
            FROM $table
            WHERE policy_holder_id=? AND policy_number=?
            LIMIT 1";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);

    // Bind pdo parameters
    $stmt->bindParam(1, $policy_holder, \PDO::PARAM_STR);
    $stmt->bindParam(2, $policy, \PDO::PARAM_STR);

    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetch(\PDO::FETCH_ASSOC);
}


function get_conjoints(\PDO $pdo, string $table, string $join)
{
    $sql = "SELECT t_1.numero_conjoint, t_1.prenoms, t_1.nom, t_1.sexe, t_1.date_naissance, t_1.etat_conjoint, t_2.numero_assure, t_2.date_lien, t_2.type_lien, t_2.created_at, t_2.updated_at 
            FROM $table as t_1
            JOIN $join as t_2
            ON t_1.numero_conjoint = t_2.numero_conjoint";

    return db_select($pdo, $sql, [], true, \PDO::FETCH_ASSOC);
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

    $conjoints  = get_conjoints($pdo, 'conjoints', 'assure_conjoints');
    $total = iterator_count($conjoints);

    printf("Migrating total of %d conjoints...\n", $total);

    foreach ($conjoints as $conjoint) {
        # code...
        $policy_holder = get_policy_holder(db_connect(function () use ($options) {
            return create_app_connection($options);
        }, $dstPdo), 'ass_policy_holders', $conjoint['numero_assure']);

        if (!is_array($policy_holder)) {
            printf("Data not found for policy holder: %s\n",  $conjoint['numero_assure']);
            continue;
        }

        // TODO: Get ass_mariage_bounds where policy_holder_id and policy_number exists
        $mariage_bound = get_mariage_bound(db_connect(function () use ($options) {
            return create_app_connection($options);
        }, $dstPdo), 'ass_mariage_bounds', $policy_holder['id'], $conjoint['numero_conjoint']);
        if ($mariage_bound) {
            // TODO: In future release, update the existing mariage bound
            printf("Mariage bound already exists for %s - %s\n", $policy_holder['sin'], $conjoint['numero_conjoint']);
            continue;
        }

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

        if ($policy_holder && !empty($policy_holder)) {
            db_insert(db_connect(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo), 'ass_mariage_bounds', [
                'id' => str_uuid(),
                'policy_holder_id' => strval($policy_holder['id']),
                'person_id' => $person_id,
                'policy_number' => strval($conjoint['numero_conjoint']),
                'bound_at' => $conjoint['date_lien'] ?? null,
                'bound_type_id' => $conjoint['type_lien'] ?? null,
                'spouce_state_id' =>  $conjoint['etat_conjoint'] ?? null,
                'created_at' => $conjoint['created_at'],
                'updated_at' => $conjoint['updated_at']
            ]);
            continue;
        }
    }
    printf(sprintf("\nThanks for using the program!\n"));
}

main(array_slice($argv, 1));
