<?php

class RowIterator implements Iterator
{
    /**
     * PDO statement to iterate over
     * @var PDOStatement
     */
    protected $stmt;
    /**
     * Iterator cursor key
     * 
     * @var int
     */
    protected $index = 0;
    /**
     * PDO Fetch mode
     * 
     * @var int
     */
    protected $mode;

    /**
     * Reference to the current element
     * 
     * @var array|\stdClass|object
     */
    protected $current;

    /**
     * Creates iterator instance
     * 
     * @param PDOStatement $stmt
     * @param int $mode
     */
    public function __construct(\PDOStatement $stmt, int $mode = \PDO::FETCH_OBJ)
    {
        $this->stmt = $stmt;
        $this->mode = $mode;
    }

    /**
     * @inheritDoc
     */
    #[\ReturnTypeWillChange]
    public function current()
    {
        return $this->current;
    }

    /**
     * @inheritDoc
     */
    #[\ReturnTypeWillChange]
    public function next(): void
    {
        ++$this->index;
    }

    /**
     * @inheritDoc
     */
    #[\ReturnTypeWillChange]
    public function key()
    {
        return $this->index;
    }

    /**
     * @inheritDoc
     */
    #[\ReturnTypeWillChange]
    public function valid(): bool
    {
        $this->current = $this->stmt->fetch($this->mode, \PDO::FETCH_ORI_ABS, $this->index);
        return false !== $this->current;
    }

    /**
     * @inheritDoc
     */
    #[\ReturnTypeWillChange]
    public function rewind(): void
    {
        // Close previously opened cursor to allow statement to be executed again
        $this->stmt->closeCursor();
        // To allow the rewind to have a new scrollable result set, we call the execute() each
        // time we rewind the iterate to have the cursor pointing to 
        $this->stmt->execute();
        // WE seek position 0 of the result data set
        $this->index = 0;
        // We set the value of the current property to null to release resource
        $this->current = null;
    }
}

/**
 * 
 * @param string $host 
 * @param string $db 
 * @param string $driver 
 * @param int $port 
 * @param string $charset 
 * @return string 
 */
function create_dsn(string $host, string $db, $driver = 'mysql', $port = 3306, $charset = 'UTF8')
{
    $port = strval($port ?? 3306);
    $charset = strval($charset ?? 'UTF8');
    $driver = strval($driver ?? 'mysql');
    return "$driver:host=$host;port=$port;dbname=$db;charset=$charset";
}

/**
 * Creates a PDO connnection instance
 * 
 * @param string $user 
 * @param string $password 
 * @param mixed $dsn_or_host 
 * @param string $driver 
 * @param mixed $dbname 
 * @param mixed $port 
 * @param string $charset 
 * @return PDO 
 */
function create_database_connection(string $user, string $password, $dsn_or_host, $driver = 'mysql', $dbname = null, $port = null, $charset = 'UTF8')
{
    try {
        $dsn = false === filter_var($dsn_or_host, FILTER_VALIDATE_IP) ? $dsn_or_host : create_dsn($dsn_or_host, $dbname, $driver, $port, $charset);
        return new PDO($dsn, $user, $password, [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION, \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_OBJ]);
    } catch (\Throwable $e) {
    }
}

/**
 * Prepare database sql statement
 * 
 * @param PDO $pdo 
 * @param string $sql 
 * @return PDOStatement 
 * @throws PDOException 
 */
function db_prepare_sql(\PDO $pdo, string $sql)
{
    // Set the attr_cursor to cursor_scroll to make the iterator usable with RowIterator
    $stmt = $pdo->prepare($sql, [\PDO::ATTR_CURSOR => \PDO::CURSOR_SCROLL]);
    if (false === $stmt) {
        list($errorCode, $_, $message) = $pdo->errorInfo();
        throw new \PDOException($message ?? "STATEMENT ERROR : $sql", $errorCode ?? 500);
    }
    return $stmt;
}

/**
 * Provides a row count interface for PDO statement
 * 
 * @param PDOStatement $stmt 
 * @return int 
 */
function db_affected_rows(\PDOStatement $stmt)
{
    return $stmt->rowCount();
}

/**
 * Execute a database query and return the query statement
 * 
 * @param PDO $pdo 
 * @param string $sql 
 * @param array $params 
 * @return PDOStatement 
 * @throws PDOException 
 */
function db_query(\PDO $pdo, string $sql, $params = [])
{
    $stmt = db_prepare_sql($pdo, $sql);
    foreach ($params as $value) {
        $stmt->bindParam(...$value);
    }
    $stmt->execute();
    return $stmt;
}

/**
 * Execute a select query against a pdo connection
 * 
 * @param PDO $pdo 
 * @param string $sql 
 * @param array $params 
 * @param bool $cursor 
 * @param int $mode
 * 
 * @return array|Iterator 
 * @throws PDOException 
 */
function db_select(\PDO $pdo, string $sql, $params = [], $cursor = true, int $mode = \PDO::FETCH_OBJ)
{
    $stmt = db_prepare_sql($pdo, $sql);
    foreach ($params as $value) {
        $stmt->bindParam(...$value);
    }
    if ($cursor) {
        // The statement will be executed each time the iterator is rewing
        return new RowIterator($stmt, $mode);
    }
    $stmt->execute();
    return (false === ($result = $stmt->fetchAll($mode))) ? [] : $result;
}

/**
 * Insert a row in the database
 * 
 * @param PDO $pdo 
 * @param mixed $table 
 * @param array $data 
 * @return mixed 
 * @throws PDOException 
 */
function db_insert(\PDO $pdo, $table, array $data)
{
    $sql = sprintf(
        "INSERT INTO %s (%s) VALUES (%s)",
        $table,
        implode(", ", array_keys($data)),
        implode(", ", array_map(function ($key) {
            return ":$key";
        }, array_keys($data)))
    );
    $stmt = $pdo->prepare($sql);
    $values = array_reduce(array_keys($data), function ($carry, $current) use ($data) {
        $carry[":$current"] = $data[$current];
        return $carry;
    }, []);

    $stmt->execute($values);

    return $stmt->fetch(\PDO::FETCH_ASSOC);
}


/**
 * Insert a list of row into the database table
 * 
 * @param PDO $pdo 
 * @param mixed $table 
 * @param array $data 
 * @return int 
 */
function db_insert_many(\PDO $pdo, $table, array $data)
{
    $start = key($data);
    $values = str_repeat('?,', count($data[$start]) - 1) . '?';
    $sql_columns = array_keys($data[$start]);
    sort($sql_columns);
    $sql = sprintf(
        "INSERT INTO %s (%s) VALUES %s",
        $table,
        implode(", ", $sql_columns),
        str_repeat("($values),", count($data) - 1) . "($values)"
    );

    // Dump the generated sql
    $result = array_map(function($current) {
        ksort($current);
        return array_values($current);
    }, $data);

    $stmt = $pdo->prepare($sql);

    $stmt->execute(array_merge(...$result));

    return $stmt->rowCount();
}

/**
 * @param callable $conn_factory 
 * @param PDO|null $connection 
 * @return PDO 
 * @throws RuntimeException 
 */
function db_connect(callable $conn_factory, \PDO $connection = null)
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