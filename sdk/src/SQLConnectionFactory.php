<?php

namespace Drewlabs\ETL;

class SQLConnectionFactory implements ConnectionFactory
{
    /**
     * @var \PDO
     */
    private $pdo;
    private  $user;
    private  $password;
    private  $driver;
    private  $dns_or_host;
    private  $port;
    private  $dbname;
    private  $charset;

    public function __construct(
        string $user = 'docker',
        string $password = 'docker',
        string $driver = 'mysql',
        string $dns_or_host = '0.0.0.0',
        int $port = 3306,
        ?string $dbname = null,
        string $charset = 'UTF8'
    ) {
        $this->user = $user;
        $this->password = $password;
        $this->driver = $driver;
        $this->dns_or_host = $dns_or_host;
        $this->port = $port;
        $this->dbname = $dbname;
        $this->charset = $charset;
    }

    /**
     * Creates sql connection from dictionnary of configuration
     * 
     * @param array $array 
     * @return static 
     */
    public static function fromArray(array $array)
    {
        return new static(
            $array['user'] ?? 'docker',
            $array['password'] ?? 'password',
            $array['driver'] ?? 'mysql',
            $array['host'] ?? '0.0.0.0',
            intval($array['port'] ?? 3306),
            $array['db'] ?? null
        );
    }

    /**
     * {@inheritDoc}
     * 
     * @return \PDO
     */
    public function createConnection()
    {
        // Reuse the pdo connection if it's already created
        if (null === $this->pdo) {
            $this->pdo = create_database_connection(
                $this->user,
                $this->password,
                $this->dns_or_host,
                $this->driver,
                $this->dbname,
                $this->port,
                $this->charset
            );
        }

        // Returns the created pdo connection
        return $this->pdo;
    }
}
