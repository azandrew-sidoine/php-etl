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


/**
 * Select users from the source database
 * @param PDO $pdo 
 * @return array|Iterator 
 * @throws PDOException 
 */
function select_users(\PDO $pdo, string $table)
{
    $sql = "
        SELECT id, username, password, numero_assurance, double_auth_active, is_active, contact
        FROM $table
        WHERE type_partenaire=?
    ";

    return db_select(
        $pdo,
        $sql,
        [
            [1, 2, \PDO::PARAM_INT],
        ],
        true,
        \PDO::FETCH_ASSOC
    );
}

/**
 * Select user from the destination database
 * 
 * @param PDO $pdo 
 * @return array|Iterator 
 * @throws PDOException 
 */
function select_user(\PDO $pdo, string $table, $user_id)
{
    $sql = "
        SELECT user_id
        FROM $table
        WHERE user_id=?
        LIMIT 1
    ";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);
    
    // Bind pdo parameters
    $stmt->bindParam(1, $user_id, \PDO::PARAM_INT);

    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetch(\PDO::FETCH_ASSOC);
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
    $program = 'Program: Migrate CNSS assure users data from source to destination database';
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

    $users  = select_users($pdo, 'users');
    $total = iterator_count($users);

    printf("Migrating total of %d users...\n", $total);

    foreach ($users as $user) {
        printf("Processing user %s records...\n", $user['username'] ?? 'Unknown');
        $result = select_user(db_connect(function () use ($options) {
            return create_dst_connection($options);
        }, $dstPdo), 'auth_users', $user['id']);

        if (!$result) {
            $result = db_insert(db_connect(function () use ($options) {
                return create_dst_connection($options);
            }, $dstPdo), 'auth_users', [
                'user_id' => $user['id'],
                'user_name' => $user['username'],
                'user_password' => $user['password'],
                'lock_enabled' => 0,
                'login_attempts' => null,
                'lock_expired_at' => null,
                'double_auth_active' => $user['double_auth_active'],
                'is_active' => $user['is_active'],
                'is_verified' => 1
            ]);
            // TODO: Insert into auth_user_details if contact is a phone number
            if (false !== filter_var($user['username'], FILTER_VALIDATE_EMAIL) && isset($user['contact'])) {
                db_insert(db_connect(function () use ($options) {
                    return create_dst_connection($options);
                }, $dstPdo), 'auth_user_details', [
                    'user_id' => $result['user_id'],
                    'phone_number' => $user['contact'],
                    'email' => $user['username']
                ]);
            }
        }

        if ($result && is_array($result)) {
            // TODO Add user to policy_holder_users table
            if(isset($user['numero_assurance'])) {
                $policy_holder = get_policy_holder(db_connect(function () use ($options) {
                    return create_app_connection($options);
                }, $appPdo), 'ass_policy_holders', $user['numero_assurance']);
                if ($policy_holder) {
                    db_insert(db_connect(function () use ($options) {
                        return create_app_connection($options);
                    }, $appPdo), 'ass_policy_holder_users', [
                        'user_id' => $result['user_id'],
                        'policy_holder_id' => $policy_holder['id'],
                        'validated' => 1
                    ]);
                }

            } else {
                printf("No numero_assure for user %s, processing next record\n", $user['username'] ?? 'Unknown');
            }
        }
    }
    printf(sprintf("\nThanks for using the program!\n"));
}

main(array_slice($argv, 1));
