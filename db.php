<?php

function &DB($params = '')
{
    $DB = new CI_DB_mysqli_driver($params);
    $DB->initialize();
    return $DB;
}

/**
 * Database Driver Class
 *
 * This is the platform-independent base DB implementation class.
 * This class will not be called directly. Rather, the adapter
 * class for the specific database will extend and instantiate it.
 *
 * @package        CodeIgniter
 * @subpackage    Drivers
 * @category    Database
 * @author        EllisLab Dev Team
 * @link        https://codeigniter.com/user_guide/database/
 */
abstract class CI_DB_driver
{

    /**
     * Data Source Name / Connect string
     *
     * @var    string
     */
    public $dsn;

    /**
     * Username
     *
     * @var    string
     */
    public $username;

    /**
     * Password
     *
     * @var    string
     */
    public $password;

    /**
     * Hostname
     *
     * @var    string
     */
    public $hostname;

    /**
     * Database name
     *
     * @var    string
     */
    public $database;

    /**
     * Database driver
     *
     * @var    string
     */
    public $dbdriver = 'mysqli';

    /**
     * Sub-driver
     *
     * @used-by    CI_DB_pdo_driver
     * @var    string
     */
    public $subdriver;

    /**
     * Table prefix
     *
     * @var    string
     */
    public $dbprefix = '';

    /**
     * Character set
     *
     * @var    string
     */
    public $char_set = 'utf8';

    /**
     * Collation
     *
     * @var    string
     */
    public $dbcollat = 'utf8_general_ci';

    /**
     * Encryption flag/data
     *
     * @var    mixed
     */
    public $encrypt = FALSE;

    /**
     * Swap Prefix
     *
     * @var    string
     */
    public $swap_pre = '';

    /**
     * Database port
     *
     * @var    int
     */
    public $port = NULL;

    /**
     * Persistent connection flag
     *
     * @var    bool
     */
    public $pconnect = FALSE;

    /**
     * Connection ID
     *
     * @var    object|resource
     */
    public $conn_id = FALSE;

    /**
     * Result ID
     *
     * @var    object|resource
     */
    public $result_id = FALSE;

    /**
     * Debug flag
     *
     * Whether to display error messages.
     *
     * @var    bool
     */
    public $db_debug = FALSE;

    /**
     * Benchmark time
     *
     * @var    int
     */
    public $benchmark = 0;

    /**
     * Executed queries count
     *
     * @var    int
     */
    public $query_count = 0;

    /**
     * Bind marker
     *
     * Character used to identify values in a prepared statement.
     *
     * @var    string
     */
    public $bind_marker = '?';

    /**
     * Save queries flag
     *
     * Whether to keep an in-memory history of queries for debugging purposes.
     *
     * @var    bool
     */
    public $save_queries = TRUE;

    /**
     * Queries list
     *
     * @see    CI_DB_driver::$save_queries
     * @var    string[]
     */
    public $queries = array();

    /**
     * Query times
     *
     * A list of times that queries took to execute.
     *
     * @var    array
     */
    public $query_times = array();

    /**
     * Data cache
     *
     * An internal generic value cache.
     *
     * @var    array
     */
    public $data_cache = array();

    /**
     * Transaction enabled flag
     *
     * @var    bool
     */
    public $trans_enabled = TRUE;

    /**
     * Strict transaction mode flag
     *
     * @var    bool
     */
    public $trans_strict = TRUE;

    /**
     * Transaction depth level
     *
     * @var    int
     */
    protected $_trans_depth = 0;

    /**
     * Transaction status flag
     *
     * Used with transactions to determine if a rollback should occur.
     *
     * @var    bool
     */
    protected $_trans_status = TRUE;

    /**
     * Transaction failure flag
     *
     * Used with transactions to determine if a transaction has failed.
     *
     * @var    bool
     */
    protected $_trans_failure = FALSE;

    /**
     * Cache On flag
     *
     * @var    bool
     */
    public $cache_on = FALSE;

    /**
     * Cache directory path
     *
     * @var    bool
     */
    public $cachedir = '';

    /**
     * Cache auto-delete flag
     *
     * @var    bool
     */
    public $cache_autodel = FALSE;

    /**
     * DB Cache object
     *
     * @see    CI_DB_cache
     * @var    object
     */
    public $CACHE;

    /**
     * Protect identifiers flag
     *
     * @var    bool
     */
    protected $_protect_identifiers = TRUE;

    /**
     * List of reserved identifiers
     *
     * Identifiers that must NOT be escaped.
     *
     * @var    string[]
     */
    protected $_reserved_identifiers = array('*');

    /**
     * Identifier escape character
     *
     * @var    string
     */
    protected $_escape_char = '"';

    /**
     * ESCAPE statement string
     *
     * @var    string
     */
    protected $_like_escape_str = " ESCAPE '%s' ";

    /**
     * ESCAPE character
     *
     * @var    string
     */
    protected $_like_escape_chr = '!';

    /**
     * ORDER BY random keyword
     *
     * @var    array
     */
    protected $_random_keyword = array('RAND()', 'RAND(%d)');

    /**
     * COUNT string
     *
     * @used-by    CI_DB_driver::count_all()
     * @used-by    CI_DB_query_builder::count_all_results()
     *
     * @var    string
     */
    protected $_count_string = 'SELECT COUNT(*) AS ';

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @param    array $params
     * @return    void
     */
    public function __construct($params)
    {
        if (is_array($params)) {
            foreach ($params as $key => $val) {
                $this->$key = $val;
            }
        }
    }

    // --------------------------------------------------------------------

    /**
     * Initialize Database Settings
     *
     * @return    bool
     */
    public function initialize()
    {
        /* If an established connection is available, then there's
         * no need to connect and select the database.
         *
         * Depending on the database driver, conn_id can be either
         * boolean TRUE, a resource or an object.
         */
        if ($this->conn_id) {
            return TRUE;
        }

        // ----------------------------------------------------------------

        // Connect to the database and set the connection ID
        $this->conn_id = $this->db_connect($this->pconnect);

        // No connection resource? Check if there is a failover else throw an error
        if (!$this->conn_id) {
            // Check if there is a failover set
            if (!empty($this->failover) && is_array($this->failover)) {
                // Go over all the failovers
                foreach ($this->failover as $failover) {
                    // Replace the current settings with those of the failover
                    foreach ($failover as $key => $val) {
                        $this->$key = $val;
                    }

                    // Try to connect
                    $this->conn_id = $this->db_connect($this->pconnect);


                    // If a connection is made break the foreach loop
                    if ($this->conn_id) {
                        break;
                    }
                }
            }


            // We still don't have a connection?
            if (!$this->conn_id) {

                if ($this->db_debug) {
                    $this->display_error('db_unable_to_connect');
                }

                return FALSE;
            }
        }

        // Now we set the character set and that's all
        return $this->db_set_charset($this->char_set);
    }

    // --------------------------------------------------------------------

    /**
     * DB connect
     *
     * This is just a dummy method that all drivers will override.
     *
     * @return    mixed
     */
    public function db_connect()
    {
        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Persistent database connection
     *
     * @return    mixed
     */
    public function db_pconnect()
    {
        return $this->db_connect(TRUE);
    }

    // --------------------------------------------------------------------

    /**
     * Reconnect
     *
     * Keep / reestablish the db connection if no queries have been
     * sent for a length of time exceeding the server's idle timeout.
     *
     * This is just a dummy method to allow drivers without such
     * functionality to not declare it, while others will override it.
     *
     * @return    void
     */
    public function reconnect()
    {
    }

    // --------------------------------------------------------------------

    /**
     * Select database
     *
     * This is just a dummy method to allow drivers without such
     * functionality to not declare it, while others will override it.
     *
     * @return    bool
     */
    public function db_select()
    {
        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Last error
     *
     * @return    array
     */
    public function error()
    {
        return array('code' => NULL, 'message' => NULL);
    }

    // --------------------------------------------------------------------

    /**
     * Set client character set
     *
     * @param    string
     * @return    bool
     */
    public function db_set_charset($charset)
    {
        if (method_exists($this, '_db_set_charset') && !$this->_db_set_charset($charset)) {

            if ($this->db_debug) {
                $this->display_error('db_unable_to_set_charset', $charset);
            }

            return FALSE;
        }

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * The name of the platform in use (mysql, mssql, etc...)
     *
     * @return    string
     */
    public function platform()
    {
        return $this->dbdriver;
    }

    // --------------------------------------------------------------------

    /**
     * Database version number
     *
     * Returns a string containing the version of the database being used.
     * Most drivers will override this method.
     *
     * @return    string
     */
    public function version()
    {
        if (isset($this->data_cache['version'])) {
            return $this->data_cache['version'];
        }

        if (FALSE === ($sql = $this->_version())) {
            return ($this->db_debug) ? $this->display_error('db_unsupported_function') : FALSE;
        }

        $query = $this->query($sql)->row();
        return $this->data_cache['version'] = $query->ver;
    }

    // --------------------------------------------------------------------

    /**
     * Version number query string
     *
     * @return    string
     */
    protected function _version()
    {
        return 'SELECT VERSION() AS ver';
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * Accepts an SQL string as input and returns a result object upon
     * successful execution of a "read" type query. Returns boolean TRUE
     * upon successful execution of a "write" type query. Returns boolean
     * FALSE upon failure, and if the $db_debug variable is set to TRUE
     * will raise an error.
     *
     * @param    string $sql
     * @param    array $binds = FALSE        An array of binding data
     * @param    bool $return_object = NULL
     * @return    mixed
     */
    public function query($sql, $binds = FALSE, $return_object = NULL)
    {
        if ($sql === '') {
            return ($this->db_debug) ? $this->display_error('db_invalid_query') : FALSE;
        } elseif (!is_bool($return_object)) {
            $return_object = !$this->is_write_type($sql);
        }

        // Verify table prefix and replace if necessary
        if ($this->dbprefix !== '' && $this->swap_pre !== '' && $this->dbprefix !== $this->swap_pre) {
            $sql = preg_replace('/(\W)' . $this->swap_pre . '(\S+?)/', '\\1' . $this->dbprefix . '\\2', $sql);
        }

        // Compile binds if needed
        if ($binds !== FALSE) {
            $sql = $this->compile_binds($sql, $binds);
        }

        // Is query caching enabled? If the query is a "read type"
        // we will load the caching class and return the previously
        // cached query if it exists
        if ($this->cache_on === TRUE && $return_object === TRUE && $this->_cache_init()) {
            $this->load_rdriver();
            if (FALSE !== ($cache = $this->CACHE->read($sql))) {
                return $cache;
            }
        }

        // Save the query for debugging
        if ($this->save_queries === TRUE) {
            $this->queries[] = $sql;
        }

        // Start the Query Timer
        $time_start = microtime(TRUE);

        // Run the Query
        if (FALSE === ($this->result_id = $this->simple_query($sql))) {
            if ($this->save_queries === TRUE) {
                $this->query_times[] = 0;
            }

            // This will trigger a rollback if transactions are being used
            if ($this->_trans_depth !== 0) {
                $this->_trans_status = FALSE;
            }

            // Grab the error now, as we might run some additional queries before displaying the error
            $error = $this->error();

            // Log errors

            if ($this->db_debug) {
                // We call this function in order to roll-back queries
                // if transactions are enabled. If we don't call this here
                // the error message will trigger an exit, causing the
                // transactions to remain in limbo.
                while ($this->_trans_depth !== 0) {
                    $trans_depth = $this->_trans_depth;
                    $this->trans_complete();
                    if ($trans_depth === $this->_trans_depth) {
                        break;
                    }
                }

                // Display errors
                return $this->display_error(array('Error Number: ' . $error['code'], $error['message'], $sql));
            }

            return FALSE;
        }

        // Stop and aggregate the query time results
        $time_end = microtime(TRUE);
        $this->benchmark += $time_end - $time_start;

        if ($this->save_queries === TRUE) {
            $this->query_times[] = $time_end - $time_start;
        }

        // Increment the query counter
        $this->query_count++;

        // Will we have a result object instantiated? If not - we'll simply return TRUE
        if ($return_object !== TRUE) {
            // If caching is enabled we'll auto-cleanup any existing files related to this particular URI
            if ($this->cache_on === TRUE && $this->cache_autodel === TRUE && $this->_cache_init()) {
                $this->CACHE->delete();
            }

            return TRUE;
        }

        // Load and instantiate the result driver
        $driver = $this->load_rdriver();
        $RES = new $driver($this);

        // Is query caching enabled? If so, we'll serialize the
        // result object and save it to a cache file.
        if ($this->cache_on === TRUE && $this->_cache_init()) {
            // We'll create a new instance of the result object
            // only without the platform specific driver since
            // we can't use it with cached data (the query result
            // resource ID won't be any good once we've cached the
            // result object, so we'll have to compile the data
            // and save it)
            $CR = new CI_DB_result($this);
            $CR->result_object = $RES->result_object();
            $CR->result_array = $RES->result_array();
            $CR->num_rows = $RES->num_rows();

            // Reset these since cached objects can not utilize resource IDs.
            $CR->conn_id = NULL;
            $CR->result_id = NULL;

            $this->CACHE->write($sql, $CR);
        }

        return $RES;
    }

    // --------------------------------------------------------------------

    /**
     * Load the result drivers
     *
     * @return    string    the name of the result class
     */
    public function load_rdriver()
    {
        $driver = 'CI_DB_mysqli_result';
        return $driver;
    }

    // --------------------------------------------------------------------

    /**
     * Simple Query
     * This is a simplified version of the query() function. Internally
     * we only use it when running transaction commands since they do
     * not require all the features of the main query() function.
     *
     * @param    string    the sql query
     * @return    mixed
     */
    public function simple_query($sql)
    {
        if (!$this->conn_id) {
            if (!$this->initialize()) {
                return FALSE;
            }
        }

        return $this->_execute($sql);
    }

    // --------------------------------------------------------------------

    /**
     * Disable Transactions
     * This permits transactions to be disabled at run-time.
     *
     * @return    void
     */
    public function trans_off()
    {
        $this->trans_enabled = FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Enable/disable Transaction Strict Mode
     *
     * When strict mode is enabled, if you are running multiple groups of
     * transactions, if one group fails all subsequent groups will be
     * rolled back.
     *
     * If strict mode is disabled, each group is treated autonomously,
     * meaning a failure of one group will not affect any others
     *
     * @param    bool $mode = TRUE
     * @return    void
     */
    public function trans_strict($mode = TRUE)
    {
        $this->trans_strict = is_bool($mode) ? $mode : TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Start Transaction
     *
     * @param    bool $test_mode = FALSE
     * @return    bool
     */
    public function trans_start($test_mode = FALSE)
    {
        if (!$this->trans_enabled) {
            return FALSE;
        }

        return $this->trans_begin($test_mode);
    }

    // --------------------------------------------------------------------

    /**
     * Complete Transaction
     *
     * @return    bool
     */
    public function trans_complete()
    {
        if (!$this->trans_enabled) {
            return FALSE;
        }

        // The query() function will set this flag to FALSE in the event that a query failed
        if ($this->_trans_status === FALSE OR $this->_trans_failure === TRUE) {
            $this->trans_rollback();

            // If we are NOT running in strict mode, we will reset
            // the _trans_status flag so that subsequent groups of
            // transactions will be permitted.
            if ($this->trans_strict === FALSE) {
                $this->_trans_status = TRUE;
            }

            return FALSE;
        }

        return $this->trans_commit();
    }

    // --------------------------------------------------------------------

    /**
     * Lets you retrieve the transaction flag to determine if it has failed
     *
     * @return    bool
     */
    public function trans_status()
    {
        return $this->_trans_status;
    }

    // --------------------------------------------------------------------

    /**
     * Begin Transaction
     *
     * @param    bool $test_mode
     * @return    bool
     */
    public function trans_begin($test_mode = FALSE)
    {
        if (!$this->trans_enabled) {
            return FALSE;
        } // When transactions are nested we only begin/commit/rollback the outermost ones
        elseif ($this->_trans_depth > 0) {
            $this->_trans_depth++;
            return TRUE;
        }

        // Reset the transaction failure flag.
        // If the $test_mode flag is set to TRUE transactions will be rolled back
        // even if the queries produce a successful result.
        $this->_trans_failure = ($test_mode === TRUE);

        if ($this->_trans_begin()) {
            $this->_trans_status = TRUE;
            $this->_trans_depth++;
            return TRUE;
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Commit Transaction
     *
     * @return    bool
     */
    public function trans_commit()
    {
        if (!$this->trans_enabled OR $this->_trans_depth === 0) {
            return FALSE;
        } // When transactions are nested we only begin/commit/rollback the outermost ones
        elseif ($this->_trans_depth > 1 OR $this->_trans_commit()) {
            $this->_trans_depth--;
            return TRUE;
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Rollback Transaction
     *
     * @return    bool
     */
    public function trans_rollback()
    {
        if (!$this->trans_enabled OR $this->_trans_depth === 0) {
            return FALSE;
        } // When transactions are nested we only begin/commit/rollback the outermost ones
        elseif ($this->_trans_depth > 1 OR $this->_trans_rollback()) {
            $this->_trans_depth--;
            return TRUE;
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Compile Bindings
     *
     * @param    string    the sql statement
     * @param    array    an array of bind data
     * @return    string
     */
    public function compile_binds($sql, $binds)
    {
        if (empty($this->bind_marker) OR strpos($sql, $this->bind_marker) === FALSE) {
            return $sql;
        } elseif (!is_array($binds)) {
            $binds = array($binds);
            $bind_count = 1;
        } else {
            // Make sure we're using numeric keys
            $binds = array_values($binds);
            $bind_count = count($binds);
        }

        // We'll need the marker length later
        $ml = strlen($this->bind_marker);

        // Make sure not to replace a chunk inside a string that happens to match the bind marker
        if ($c = preg_match_all("/'[^']*'|\"[^\"]*\"/i", $sql, $matches)) {
            $c = preg_match_all('/' . preg_quote($this->bind_marker, '/') . '/i',
                str_replace($matches[0],
                    str_replace($this->bind_marker, str_repeat(' ', $ml), $matches[0]),
                    $sql, $c),
                $matches, PREG_OFFSET_CAPTURE);

            // Bind values' count must match the count of markers in the query
            if ($bind_count !== $c) {
                return $sql;
            }
        } elseif (($c = preg_match_all('/' . preg_quote($this->bind_marker, '/') . '/i', $sql, $matches, PREG_OFFSET_CAPTURE)) !== $bind_count) {
            return $sql;
        }

        do {
            $c--;
            $escaped_value = $this->escape($binds[$c]);
            if (is_array($escaped_value)) {
                $escaped_value = '(' . implode(',', $escaped_value) . ')';
            }
            $sql = substr_replace($sql, $escaped_value, $matches[0][$c][1], $ml);
        } while ($c !== 0);

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Determines if a query is a "write" type.
     *
     * @param    string    An SQL query string
     * @return    bool
     */
    public function is_write_type($sql)
    {
        return (bool)preg_match('/^\s*"?(SET|INSERT|UPDATE|DELETE|REPLACE|CREATE|DROP|TRUNCATE|LOAD|COPY|ALTER|RENAME|GRANT|REVOKE|LOCK|UNLOCK|REINDEX|MERGE)\s/i', $sql);
    }

    // --------------------------------------------------------------------

    /**
     * Calculate the aggregate query elapsed time
     *
     * @param    int    The number of decimal places
     * @return    string
     */
    public function elapsed_time($decimals = 6)
    {
        return number_format($this->benchmark, $decimals);
    }

    // --------------------------------------------------------------------

    /**
     * Returns the total number of queries
     *
     * @return    int
     */
    public function total_queries()
    {
        return $this->query_count;
    }

    // --------------------------------------------------------------------

    /**
     * Returns the last query that was executed
     *
     * @return    string
     */
    public function last_query()
    {
        return end($this->queries);
    }

    // --------------------------------------------------------------------

    /**
     * "Smart" Escape String
     *
     * Escapes data based on type
     * Sets boolean and null types
     *
     * @param    string
     * @return    mixed
     */
    public function escape($str)
    {
        if (is_array($str)) {
            $str = array_map(array(&$this, 'escape'), $str);
            return $str;
        } elseif (is_string($str) OR (is_object($str) && method_exists($str, '__toString'))) {
            return "'" . $this->escape_str($str) . "'";
        } elseif (is_bool($str)) {
            return ($str === FALSE) ? 0 : 1;
        } elseif ($str === NULL) {
            return 'NULL';
        }

        return $str;
    }

    // --------------------------------------------------------------------

    /**
     * Escape String
     *
     * @param    string|string[] $str Input string
     * @param    bool $like Whether or not the string will be used in a LIKE condition
     * @return    string
     */
    public function escape_str($str, $like = FALSE)
    {
        if (is_array($str)) {
            foreach ($str as $key => $val) {
                $str[$key] = $this->escape_str($val, $like);
            }

            return $str;
        }

        $str = $this->_escape_str($str);

        // escape LIKE condition wildcards
        if ($like === TRUE) {
            return str_replace(
                array($this->_like_escape_chr, '%', '_'),
                array($this->_like_escape_chr . $this->_like_escape_chr, $this->_like_escape_chr . '%', $this->_like_escape_chr . '_'),
                $str
            );
        }

        return $str;
    }

    // --------------------------------------------------------------------

    /**
     * Escape LIKE String
     *
     * Calls the individual driver for platform
     * specific escaping for LIKE conditions
     *
     * @param    string|string[]
     * @return    mixed
     */
    public function escape_like_str($str)
    {
        return $this->escape_str($str, TRUE);
    }

    // --------------------------------------------------------------------

    /**
     * Platform-dependent string escape
     *
     * @param    string
     * @return    string
     */
    protected function _escape_str($str)
    {
        return str_replace("'", "''", remove_invisible_characters($str, FALSE));
    }

    // --------------------------------------------------------------------

    /**
     * Primary
     *
     * Retrieves the primary key. It assumes that the row in the first
     * position is the primary key
     *
     * @param    string $table Table name
     * @return    string
     */
    public function primary($table)
    {
        $fields = $this->list_fields($table);
        return is_array($fields) ? current($fields) : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * "Count All" query
     *
     * Generates a platform-specific query string that counts all records in
     * the specified database
     *
     * @param    string
     * @return    int
     */
    public function count_all($table = '')
    {
        if ($table === '') {
            return 0;
        }

        $query = $this->query($this->_count_string . $this->escape_identifiers('numrows') . ' FROM ' . $this->protect_identifiers($table, TRUE, NULL, FALSE));
        if ($query->num_rows() === 0) {
            return 0;
        }

        $query = $query->row();
        $this->_reset_select();
        return (int)$query->numrows;
    }

    // --------------------------------------------------------------------

    /**
     * Returns an array of table names
     *
     * @param    string $constrain_by_prefix = FALSE
     * @return    array
     */
    public function list_tables($constrain_by_prefix = FALSE)
    {
        // Is there a cached result?
        if (isset($this->data_cache['table_names'])) {
            return $this->data_cache['table_names'];
        }

        if (FALSE === ($sql = $this->_list_tables($constrain_by_prefix))) {
            return ($this->db_debug) ? $this->display_error('db_unsupported_function') : FALSE;
        }

        $this->data_cache['table_names'] = array();
        $query = $this->query($sql);

        foreach ($query->result_array() as $row) {
            // Do we know from which column to get the table name?
            if (!isset($key)) {
                if (isset($row['table_name'])) {
                    $key = 'table_name';
                } elseif (isset($row['TABLE_NAME'])) {
                    $key = 'TABLE_NAME';
                } else {
                    /* We have no other choice but to just get the first element's key.
                     * Due to array_shift() accepting its argument by reference, if
                     * E_STRICT is on, this would trigger a warning. So we'll have to
                     * assign it first.
                     */
                    $key = array_keys($row);
                    $key = array_shift($key);
                }
            }

            $this->data_cache['table_names'][] = $row[$key];
        }

        return $this->data_cache['table_names'];
    }

    // --------------------------------------------------------------------

    /**
     * Determine if a particular table exists
     *
     * @param    string $table_name
     * @return    bool
     */
    public function table_exists($table_name)
    {
        return in_array($this->protect_identifiers($table_name, TRUE, FALSE, FALSE), $this->list_tables());
    }

    // --------------------------------------------------------------------

    /**
     * Fetch Field Names
     *
     * @param    string $table Table name
     * @return    array
     */
    public function list_fields($table)
    {
        if (FALSE === ($sql = $this->_list_columns($table))) {
            return ($this->db_debug) ? $this->display_error('db_unsupported_function') : FALSE;
        }

        $query = $this->query($sql);
        $fields = array();

        foreach ($query->result_array() as $row) {
            // Do we know from where to get the column's name?
            if (!isset($key)) {
                if (isset($row['column_name'])) {
                    $key = 'column_name';
                } elseif (isset($row['COLUMN_NAME'])) {
                    $key = 'COLUMN_NAME';
                } else {
                    // We have no other choice but to just get the first element's key.
                    $key = key($row);
                }
            }

            $fields[] = $row[$key];
        }

        return $fields;
    }

    // --------------------------------------------------------------------

    /**
     * Determine if a particular field exists
     *
     * @param    string
     * @param    string
     * @return    bool
     */
    public function field_exists($field_name, $table_name)
    {
        return in_array($field_name, $this->list_fields($table_name));
    }

    // --------------------------------------------------------------------

    /**
     * Returns an object with field data
     *
     * @param    string $table the table name
     * @return    array
     */
    public function field_data($table)
    {
        $query = $this->query($this->_field_data($this->protect_identifiers($table, TRUE, NULL, FALSE)));
        return ($query) ? $query->field_data() : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Escape the SQL Identifiers
     *
     * This function escapes column and table names
     *
     * @param    mixed
     * @return    mixed
     */
    public function escape_identifiers($item)
    {
        if ($this->_escape_char === '' OR empty($item) OR in_array($item, $this->_reserved_identifiers)) {
            return $item;
        } elseif (is_array($item)) {
            foreach ($item as $key => $value) {
                $item[$key] = $this->escape_identifiers($value);
            }

            return $item;
        } // Avoid breaking functions and literal values inside queries
        elseif (ctype_digit($item) OR $item[0] === "'" OR ($this->_escape_char !== '"' && $item[0] === '"') OR strpos($item, '(') !== FALSE) {
            return $item;
        }

        static $preg_ec = array();

        if (empty($preg_ec)) {
            if (is_array($this->_escape_char)) {
                $preg_ec = array(
                    preg_quote($this->_escape_char[0], '/'),
                    preg_quote($this->_escape_char[1], '/'),
                    $this->_escape_char[0],
                    $this->_escape_char[1]
                );
            } else {
                $preg_ec[0] = $preg_ec[1] = preg_quote($this->_escape_char, '/');
                $preg_ec[2] = $preg_ec[3] = $this->_escape_char;
            }
        }

        foreach ($this->_reserved_identifiers as $id) {
            if (strpos($item, '.' . $id) !== FALSE) {
                return preg_replace('/' . $preg_ec[0] . '?([^' . $preg_ec[1] . '\.]+)' . $preg_ec[1] . '?\./i', $preg_ec[2] . '$1' . $preg_ec[3] . '.', $item);
            }
        }

        return preg_replace('/' . $preg_ec[0] . '?([^' . $preg_ec[1] . '\.]+)' . $preg_ec[1] . '?(\.)?/i', $preg_ec[2] . '$1' . $preg_ec[3] . '$2', $item);
    }

    // --------------------------------------------------------------------

    /**
     * Generate an insert string
     *
     * @param    string    the table upon which the query will be performed
     * @param    array    an associative array data of key/values
     * @return    string
     */
    public function insert_string($table, $data)
    {
        $fields = $values = array();

        foreach ($data as $key => $val) {
            $fields[] = $this->escape_identifiers($key);
            $values[] = $this->escape($val);
        }

        return $this->_insert($this->protect_identifiers($table, TRUE, NULL, FALSE), $fields, $values);
    }

    // --------------------------------------------------------------------

    /**
     * Insert statement
     *
     * Generates a platform-specific insert string from the supplied data
     *
     * @param    string    the table name
     * @param    array    the insert keys
     * @param    array    the insert values
     * @return    string
     */
    protected function _insert($table, $keys, $values)
    {
        return 'INSERT INTO ' . $table . ' (' . implode(', ', $keys) . ') VALUES (' . implode(', ', $values) . ')';
    }

    // --------------------------------------------------------------------

    /**
     * Generate an update string
     *
     * @param    string    the table upon which the query will be performed
     * @param    array    an associative array data of key/values
     * @param    mixed    the "where" statement
     * @return    string
     */
    public function update_string($table, $data, $where)
    {
        if (empty($where)) {
            return FALSE;
        }

        $this->where($where);

        $fields = array();
        foreach ($data as $key => $val) {
            $fields[$this->protect_identifiers($key)] = $this->escape($val);
        }

        $sql = $this->_update($this->protect_identifiers($table, TRUE, NULL, FALSE), $fields);
        $this->_reset_write();
        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Update statement
     *
     * Generates a platform-specific update string from the supplied data
     *
     * @param    string    the table name
     * @param    array    the update data
     * @return    string
     */
    protected function _update($table, $values)
    {
        foreach ($values as $key => $val) {
            $valstr[] = $key . ' = ' . $val;
        }

        return 'UPDATE ' . $table . ' SET ' . implode(', ', $valstr)
            . $this->_compile_wh('qb_where')
            . $this->_compile_order_by()
            . ($this->qb_limit !== FALSE ? ' LIMIT ' . $this->qb_limit : '');
    }

    // --------------------------------------------------------------------

    /**
     * Tests whether the string has an SQL operator
     *
     * @param    string
     * @return    bool
     */
    protected function _has_operator($str)
    {
        return (bool)preg_match('/(<|>|!|=|\sIS NULL|\sIS NOT NULL|\sEXISTS|\sBETWEEN|\sLIKE|\sIN\s*\(|\s)/i', trim($str));
    }

    // --------------------------------------------------------------------

    /**
     * Returns the SQL string operator
     *
     * @param    string
     * @return    string
     */
    protected function _get_operator($str)
    {
        static $_operators;

        if (empty($_operators)) {
            $_les = ($this->_like_escape_str !== '')
                ? '\s+' . preg_quote(trim(sprintf($this->_like_escape_str, $this->_like_escape_chr)), '/')
                : '';
            $_operators = array(
                '\s*(?:<|>|!)?=\s*',             // =, <=, >=, !=
                '\s*<>?\s*',                     // <, <>
                '\s*>\s*',                       // >
                '\s+IS NULL',                    // IS NULL
                '\s+IS NOT NULL',                // IS NOT NULL
                '\s+EXISTS\s*\(.*\)',        // EXISTS(sql)
                '\s+NOT EXISTS\s*\(.*\)',    // NOT EXISTS(sql)
                '\s+BETWEEN\s+',                 // BETWEEN value AND value
                '\s+IN\s*\(.*\)',            // IN(list)
                '\s+NOT IN\s*\(.*\)',        // NOT IN (list)
                '\s+LIKE\s+\S.*(' . $_les . ')?',    // LIKE 'expr'[ ESCAPE '%s']
                '\s+NOT LIKE\s+\S.*(' . $_les . ')?' // NOT LIKE 'expr'[ ESCAPE '%s']
            );

        }

        return preg_match('/' . implode('|', $_operators) . '/i', $str, $match)
            ? $match[0] : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Enables a native PHP function to be run, using a platform agnostic wrapper.
     *
     * @param    string $function Function name
     * @return    mixed
     */
    public function call_function($function)
    {
        $driver = ($this->dbdriver === 'postgre') ? 'pg_' : $this->dbdriver . '_';

        if (FALSE === strpos($driver, $function)) {
            $function = $driver . $function;
        }

        if (!function_exists($function)) {
            return ($this->db_debug) ? $this->display_error('db_unsupported_function') : FALSE;
        }

        return (func_num_args() > 1)
            ? call_user_func_array($function, array_slice(func_get_args(), 1))
            : call_user_func($function);
    }

    // --------------------------------------------------------------------

    /**
     * Set Cache Directory Path
     *
     * @param    string    the path to the cache directory
     * @return    void
     */
    public function cache_set_path($path = '')
    {
        $this->cachedir = $path;
    }

    // --------------------------------------------------------------------

    /**
     * Enable Query Caching
     *
     * @return    bool    cache_on value
     */
    public function cache_on()
    {
        return $this->cache_on = TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Disable Query Caching
     *
     * @return    bool    cache_on value
     */
    public function cache_off()
    {
        return $this->cache_on = FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Delete the cache files associated with a particular URI
     *
     * @param    string $segment_one = ''
     * @param    string $segment_two = ''
     * @return    bool
     */
    public function cache_delete($segment_one = '', $segment_two = '')
    {
        return $this->_cache_init()
            ? $this->CACHE->delete($segment_one, $segment_two)
            : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Delete All cache files
     *
     * @return    bool
     */
    public function cache_delete_all()
    {
        return $this->_cache_init()
            ? $this->CACHE->delete_all()
            : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Initialize the Cache Class
     *
     * @return    bool
     */
    protected function _cache_init()
    {
        if (is_object($this->CACHE)) {
            return TRUE;
        }

        $this->CACHE = new CI_DB_Cache($this); // pass db object to support multiple db connections and returned db objects
        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Close DB Connection
     *
     * @return    void
     */
    public function close()
    {
        if ($this->conn_id) {
            $this->_close();
            $this->conn_id = FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Close DB Connection
     *
     * This method would be overridden by most of the drivers.
     *
     * @return    void
     */
    protected function _close()
    {
        $this->conn_id = FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Display an error message
     *
     * @param    string    the error message
     * @param    string    any "swap" values
     * @param    bool    whether to localize the message
     * @return    string    sends the application/views/errors/error_db.php template
     */
    public function display_error($error = '', $swap = '', $native = FALSE)
    {
        echo $error;
        die;
    }

    // --------------------------------------------------------------------

    /**
     * Protect Identifiers
     *
     * This function is used extensively by the Query Builder class, and by
     * a couple functions in this class.
     * It takes a column or table name (optionally with an alias) and inserts
     * the table prefix onto it. Some logic is necessary in order to deal with
     * column names that include the path. Consider a query like this:
     *
     * SELECT hostname.database.table.column AS c FROM hostname.database.table
     *
     * Or a query with aliasing:
     *
     * SELECT m.member_id, m.member_name FROM members AS m
     *
     * Since the column name can include up to four segments (host, DB, table, column)
     * or also have an alias prefix, we need to do a bit of work to figure this out and
     * insert the table prefix (if it exists) in the proper position, and escape only
     * the correct identifiers.
     *
     * @param    string
     * @param    bool
     * @param    mixed
     * @param    bool
     * @return    string
     */
    public function protect_identifiers($item, $prefix_single = FALSE, $protect_identifiers = NULL, $field_exists = TRUE)
    {
        if (!is_bool($protect_identifiers)) {
            $protect_identifiers = $this->_protect_identifiers;
        }

        if (is_array($item)) {
            $escaped_array = array();
            foreach ($item as $k => $v) {
                $escaped_array[$this->protect_identifiers($k)] = $this->protect_identifiers($v, $prefix_single, $protect_identifiers, $field_exists);
            }

            return $escaped_array;
        }

        // This is basically a bug fix for queries that use MAX, MIN, etc.
        // If a parenthesis is found we know that we do not need to
        // escape the data or add a prefix. There's probably a more graceful
        // way to deal with this, but I'm not thinking of it -- Rick
        //
        // Added exception for single quotes as well, we don't want to alter
        // literal strings. -- Narf
        if (strcspn($item, "()'") !== strlen($item)) {
            return $item;
        }

        // Convert tabs or multiple spaces into single spaces
        $item = preg_replace('/\s+/', ' ', trim($item));

        // If the item has an alias declaration we remove it and set it aside.
        // Note: strripos() is used in order to support spaces in table names
        if ($offset = strripos($item, ' AS ')) {
            $alias = ($protect_identifiers)
                ? substr($item, $offset, 4) . $this->escape_identifiers(substr($item, $offset + 4))
                : substr($item, $offset);
            $item = substr($item, 0, $offset);
        } elseif ($offset = strrpos($item, ' ')) {
            $alias = ($protect_identifiers)
                ? ' ' . $this->escape_identifiers(substr($item, $offset + 1))
                : substr($item, $offset);
            $item = substr($item, 0, $offset);
        } else {
            $alias = '';
        }

        // Break the string apart if it contains periods, then insert the table prefix
        // in the correct location, assuming the period doesn't indicate that we're dealing
        // with an alias. While we're at it, we will escape the components
        if (strpos($item, '.') !== FALSE) {
            $parts = explode('.', $item);

            // Does the first segment of the exploded item match
            // one of the aliases previously identified? If so,
            // we have nothing more to do other than escape the item
            //
            // NOTE: The ! empty() condition prevents this method
            //       from breaking when QB isn't enabled.
            if (!empty($this->qb_aliased_tables) && in_array($parts[0], $this->qb_aliased_tables)) {
                if ($protect_identifiers === TRUE) {
                    foreach ($parts as $key => $val) {
                        if (!in_array($val, $this->_reserved_identifiers)) {
                            $parts[$key] = $this->escape_identifiers($val);
                        }
                    }

                    $item = implode('.', $parts);
                }

                return $item . $alias;
            }

            // Is there a table prefix defined in the config file? If not, no need to do anything
            if ($this->dbprefix !== '') {
                // We now add the table prefix based on some logic.
                // Do we have 4 segments (hostname.database.table.column)?
                // If so, we add the table prefix to the column name in the 3rd segment.
                if (isset($parts[3])) {
                    $i = 2;
                }
                // Do we have 3 segments (database.table.column)?
                // If so, we add the table prefix to the column name in 2nd position
                elseif (isset($parts[2])) {
                    $i = 1;
                }
                // Do we have 2 segments (table.column)?
                // If so, we add the table prefix to the column name in 1st segment
                else {
                    $i = 0;
                }

                // This flag is set when the supplied $item does not contain a field name.
                // This can happen when this function is being called from a JOIN.
                if ($field_exists === FALSE) {
                    $i++;
                }

                // dbprefix may've already been applied, with or without the identifier escaped
                $ec = '(?<ec>' . preg_quote(is_array($this->_escape_char) ? $this->_escape_char[0] : $this->_escape_char) . ')?';
                isset($ec[0]) && $ec .= '?'; // Just in case someone has disabled escaping by forcing an empty escape character

                // Verify table prefix and replace if necessary
                if ($this->swap_pre !== '' && preg_match('#^' . $ec . preg_quote($this->swap_pre) . '#', $parts[$i])) {
                    $parts[$i] = preg_replace('#^' . $ec . preg_quote($this->swap_pre) . '(\S+?)#', '\\1' . $this->dbprefix . '\\2', $parts[$i]);
                } // We only add the table prefix if it does not already exist
                else {
                    preg_match('#^' . $ec . preg_quote($this->dbprefix) . '#', $parts[$i]) OR $parts[$i] = $this->dbprefix . $parts[$i];
                }

                // Put the parts back together
                $item = implode('.', $parts);
            }

            if ($protect_identifiers === TRUE) {
                $item = $this->escape_identifiers($item);
            }

            return $item . $alias;
        }

        // Is there a table prefix? If not, no need to insert it
        if ($this->dbprefix !== '') {
            // Verify table prefix and replace if necessary
            if ($this->swap_pre !== '' && strpos($item, $this->swap_pre) === 0) {
                $item = preg_replace('/^' . $this->swap_pre . '(\S+?)/', $this->dbprefix . '\\1', $item);
            } // Do we prefix an item with no segments?
            elseif ($prefix_single === TRUE && strpos($item, $this->dbprefix) !== 0) {
                $item = $this->dbprefix . $item;
            }
        }

        if ($protect_identifiers === TRUE && !in_array($item, $this->_reserved_identifiers)) {
            $item = $this->escape_identifiers($item);
        }

        return $item . $alias;
    }

    // --------------------------------------------------------------------

    /**
     * Dummy method that allows Query Builder class to be disabled
     * and keep count_all() working.
     *
     * @return    void
     */
    protected function _reset_select()
    {
    }

}


/**
 * Query Builder Class
 *
 * This is the platform-independent base Query Builder implementation class.
 *
 * @package        CodeIgniter
 * @subpackage    Drivers
 * @category    Database
 * @author        EllisLab Dev Team
 * @link        https://codeigniter.com/user_guide/database/
 */
abstract class CI_DB_query_builder extends CI_DB_driver
{

    /**
     * Return DELETE SQL flag
     *
     * @var    bool
     */
    protected $return_delete_sql = FALSE;

    /**
     * Reset DELETE data flag
     *
     * @var    bool
     */
    protected $reset_delete_data = FALSE;

    /**
     * QB SELECT data
     *
     * @var    array
     */
    protected $qb_select = array();

    /**
     * QB DISTINCT flag
     *
     * @var    bool
     */
    protected $qb_distinct = FALSE;

    /**
     * QB FROM data
     *
     * @var    array
     */
    protected $qb_from = array();

    /**
     * QB JOIN data
     *
     * @var    array
     */
    protected $qb_join = array();

    /**
     * QB WHERE data
     *
     * @var    array
     */
    protected $qb_where = array();

    /**
     * QB GROUP BY data
     *
     * @var    array
     */
    protected $qb_groupby = array();

    /**
     * QB HAVING data
     *
     * @var    array
     */
    protected $qb_having = array();

    /**
     * QB keys
     *
     * @var    array
     */
    protected $qb_keys = array();

    /**
     * QB LIMIT data
     *
     * @var    int
     */
    protected $qb_limit = FALSE;

    /**
     * QB OFFSET data
     *
     * @var    int
     */
    protected $qb_offset = FALSE;

    /**
     * QB ORDER BY data
     *
     * @var    array
     */
    protected $qb_orderby = array();

    /**
     * QB data sets
     *
     * @var    array
     */
    protected $qb_set = array();

    /**
     * QB data set for update_batch()
     *
     * @var    array
     */
    protected $qb_set_ub = array();

    /**
     * QB aliased tables list
     *
     * @var    array
     */
    protected $qb_aliased_tables = array();

    /**
     * QB WHERE group started flag
     *
     * @var    bool
     */
    protected $qb_where_group_started = FALSE;

    /**
     * QB WHERE group count
     *
     * @var    int
     */
    protected $qb_where_group_count = 0;

    // Query Builder Caching variables

    /**
     * QB Caching flag
     *
     * @var    bool
     */
    protected $qb_caching = FALSE;

    /**
     * QB Cache exists list
     *
     * @var    array
     */
    protected $qb_cache_exists = array();

    /**
     * QB Cache SELECT data
     *
     * @var    array
     */
    protected $qb_cache_select = array();

    /**
     * QB Cache FROM data
     *
     * @var    array
     */
    protected $qb_cache_from = array();

    /**
     * QB Cache JOIN data
     *
     * @var    array
     */
    protected $qb_cache_join = array();

    /**
     * QB Cache aliased tables list
     *
     * @var    array
     */
    protected $qb_cache_aliased_tables = array();

    /**
     * QB Cache WHERE data
     *
     * @var    array
     */
    protected $qb_cache_where = array();

    /**
     * QB Cache GROUP BY data
     *
     * @var    array
     */
    protected $qb_cache_groupby = array();

    /**
     * QB Cache HAVING data
     *
     * @var    array
     */
    protected $qb_cache_having = array();

    /**
     * QB Cache ORDER BY data
     *
     * @var    array
     */
    protected $qb_cache_orderby = array();

    /**
     * QB Cache data sets
     *
     * @var    array
     */
    protected $qb_cache_set = array();

    /**
     * QB No Escape data
     *
     * @var    array
     */
    protected $qb_no_escape = array();

    /**
     * QB Cache No Escape data
     *
     * @var    array
     */
    protected $qb_cache_no_escape = array();

    // --------------------------------------------------------------------

    /**
     * Select
     *
     * Generates the SELECT portion of the query
     *
     * @param    string
     * @param    mixed
     * @return    CI_DB_query_builder
     */
    public function select($select = '*', $escape = NULL)
    {
        if (is_string($select)) {
            $select = explode(',', $select);
        }

        // If the escape value was not set, we will base it on the global setting
        is_bool($escape) OR $escape = $this->_protect_identifiers;

        foreach ($select as $val) {
            $val = trim($val);

            if ($val !== '') {
                $this->qb_select[] = $val;
                $this->qb_no_escape[] = $escape;

                if ($this->qb_caching === TRUE) {
                    $this->qb_cache_select[] = $val;
                    $this->qb_cache_exists[] = 'select';
                    $this->qb_cache_no_escape[] = $escape;
                }
            }
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Select Max
     *
     * Generates a SELECT MAX(field) portion of a query
     *
     * @param    string    the field
     * @param    string    an alias
     * @return    CI_DB_query_builder
     */
    public function select_max($select = '', $alias = '')
    {
        return $this->_max_min_avg_sum($select, $alias, 'MAX');
    }

    // --------------------------------------------------------------------

    /**
     * Select Min
     *
     * Generates a SELECT MIN(field) portion of a query
     *
     * @param    string    the field
     * @param    string    an alias
     * @return    CI_DB_query_builder
     */
    public function select_min($select = '', $alias = '')
    {
        return $this->_max_min_avg_sum($select, $alias, 'MIN');
    }

    // --------------------------------------------------------------------

    /**
     * Select Average
     *
     * Generates a SELECT AVG(field) portion of a query
     *
     * @param    string    the field
     * @param    string    an alias
     * @return    CI_DB_query_builder
     */
    public function select_avg($select = '', $alias = '')
    {
        return $this->_max_min_avg_sum($select, $alias, 'AVG');
    }

    // --------------------------------------------------------------------

    /**
     * Select Sum
     *
     * Generates a SELECT SUM(field) portion of a query
     *
     * @param    string    the field
     * @param    string    an alias
     * @return    CI_DB_query_builder
     */
    public function select_sum($select = '', $alias = '')
    {
        return $this->_max_min_avg_sum($select, $alias, 'SUM');
    }

    // --------------------------------------------------------------------

    /**
     * SELECT [MAX|MIN|AVG|SUM]()
     *
     * @used-by    select_max()
     * @used-by    select_min()
     * @used-by    select_avg()
     * @used-by    select_sum()
     *
     * @param    string $select Field name
     * @param    string $alias
     * @param    string $type
     * @return    CI_DB_query_builder
     */
    protected function _max_min_avg_sum($select = '', $alias = '', $type = 'MAX')
    {
        if (!is_string($select) OR $select === '') {
            $this->display_error('db_invalid_query');
        }

        $type = strtoupper($type);

        if (!in_array($type, array('MAX', 'MIN', 'AVG', 'SUM'))) {
            show_error('Invalid function type: ' . $type);
        }

        if ($alias === '') {
            $alias = $this->_create_alias_from_table(trim($select));
        }

        $sql = $type . '(' . $this->protect_identifiers(trim($select)) . ') AS ' . $this->escape_identifiers(trim($alias));

        $this->qb_select[] = $sql;
        $this->qb_no_escape[] = NULL;

        if ($this->qb_caching === TRUE) {
            $this->qb_cache_select[] = $sql;
            $this->qb_cache_exists[] = 'select';
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Determines the alias name based on the table
     *
     * @param    string $item
     * @return    string
     */
    protected function _create_alias_from_table($item)
    {
        if (strpos($item, '.') !== FALSE) {
            $item = explode('.', $item);
            return end($item);
        }

        return $item;
    }

    // --------------------------------------------------------------------

    /**
     * DISTINCT
     *
     * Sets a flag which tells the query string compiler to add DISTINCT
     *
     * @param    bool $val
     * @return    CI_DB_query_builder
     */
    public function distinct($val = TRUE)
    {
        $this->qb_distinct = is_bool($val) ? $val : TRUE;
        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * From
     *
     * Generates the FROM portion of the query
     *
     * @param    mixed $from can be a string or array
     * @return    CI_DB_query_builder
     */
    public function from($from)
    {
        foreach ((array)$from as $val) {
            if (strpos($val, ',') !== FALSE) {
                foreach (explode(',', $val) as $v) {
                    $v = trim($v);
                    $this->_track_aliases($v);

                    $this->qb_from[] = $v = $this->protect_identifiers($v, TRUE, NULL, FALSE);

                    if ($this->qb_caching === TRUE) {
                        $this->qb_cache_from[] = $v;
                        $this->qb_cache_exists[] = 'from';
                    }
                }
            } else {
                $val = trim($val);

                // Extract any aliases that might exist. We use this information
                // in the protect_identifiers to know whether to add a table prefix
                $this->_track_aliases($val);

                $this->qb_from[] = $val = $this->protect_identifiers($val, TRUE, NULL, FALSE);

                if ($this->qb_caching === TRUE) {
                    $this->qb_cache_from[] = $val;
                    $this->qb_cache_exists[] = 'from';
                }
            }
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * JOIN
     *
     * Generates the JOIN portion of the query
     *
     * @param    string
     * @param    string    the join condition
     * @param    string    the type of join
     * @param    string    whether not to try to escape identifiers
     * @return    CI_DB_query_builder
     */
    public function join($table, $cond, $type = '', $escape = NULL)
    {
        if ($type !== '') {
            $type = strtoupper(trim($type));

            if (!in_array($type, array('LEFT', 'RIGHT', 'OUTER', 'INNER', 'LEFT OUTER', 'RIGHT OUTER'), TRUE)) {
                $type = '';
            } else {
                $type .= ' ';
            }
        }

        // Extract any aliases that might exist. We use this information
        // in the protect_identifiers to know whether to add a table prefix
        $this->_track_aliases($table);

        is_bool($escape) OR $escape = $this->_protect_identifiers;

        if (!$this->_has_operator($cond)) {
            $cond = ' USING (' . ($escape ? $this->escape_identifiers($cond) : $cond) . ')';
        } elseif ($escape === FALSE) {
            $cond = ' ON ' . $cond;
        } else {
            // Split multiple conditions
            if (preg_match_all('/\sAND\s|\sOR\s/i', $cond, $joints, PREG_OFFSET_CAPTURE)) {
                $conditions = array();
                $joints = $joints[0];
                array_unshift($joints, array('', 0));

                for ($i = count($joints) - 1, $pos = strlen($cond); $i >= 0; $i--) {
                    $joints[$i][1] += strlen($joints[$i][0]); // offset
                    $conditions[$i] = substr($cond, $joints[$i][1], $pos - $joints[$i][1]);
                    $pos = $joints[$i][1] - strlen($joints[$i][0]);
                    $joints[$i] = $joints[$i][0];
                }
            } else {
                $conditions = array($cond);
                $joints = array('');
            }

            $cond = ' ON ';
            for ($i = 0, $c = count($conditions); $i < $c; $i++) {
                $operator = $this->_get_operator($conditions[$i]);
                $cond .= $joints[$i];
                $cond .= preg_match("/(\(*)?([\[\]\w\.'-]+)" . preg_quote($operator) . "(.*)/i", $conditions[$i], $match)
                    ? $match[1] . $this->protect_identifiers($match[2]) . $operator . $this->protect_identifiers($match[3])
                    : $conditions[$i];
            }
        }

        // Do we want to escape the table name?
        if ($escape === TRUE) {
            $table = $this->protect_identifiers($table, TRUE, NULL, FALSE);
        }

        // Assemble the JOIN statement
        $this->qb_join[] = $join = $type . 'JOIN ' . $table . $cond;

        if ($this->qb_caching === TRUE) {
            $this->qb_cache_join[] = $join;
            $this->qb_cache_exists[] = 'join';
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * WHERE
     *
     * Generates the WHERE portion of the query.
     * Separates multiple calls with 'AND'.
     *
     * @param    mixed
     * @param    mixed
     * @param    bool
     * @return    CI_DB_query_builder
     */
    public function where($key, $value = NULL, $escape = NULL)
    {
        return $this->_wh('qb_where', $key, $value, 'AND ', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * OR WHERE
     *
     * Generates the WHERE portion of the query.
     * Separates multiple calls with 'OR'.
     *
     * @param    mixed
     * @param    mixed
     * @param    bool
     * @return    CI_DB_query_builder
     */
    public function or_where($key, $value = NULL, $escape = NULL)
    {
        return $this->_wh('qb_where', $key, $value, 'OR ', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * WHERE, HAVING
     *
     * @used-by    where()
     * @used-by    or_where()
     * @used-by    having()
     * @used-by    or_having()
     *
     * @param    string $qb_key 'qb_where' or 'qb_having'
     * @param    mixed $key
     * @param    mixed $value
     * @param    string $type
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    protected function _wh($qb_key, $key, $value = NULL, $type = 'AND ', $escape = NULL)
    {
        $qb_cache_key = ($qb_key === 'qb_having') ? 'qb_cache_having' : 'qb_cache_where';

        if (!is_array($key)) {
            $key = array($key => $value);
        }

        // If the escape value was not set will base it on the global setting
        is_bool($escape) OR $escape = $this->_protect_identifiers;

        foreach ($key as $k => $v) {
            $prefix = (count($this->$qb_key) === 0 && count($this->$qb_cache_key) === 0)
                ? $this->_group_get_type('')
                : $this->_group_get_type($type);

            if ($v !== NULL) {
                if ($escape === TRUE) {
                    $v = $this->escape($v);
                }

                if (!$this->_has_operator($k)) {
                    $k .= ' = ';
                }
            } elseif (!$this->_has_operator($k)) {
                // value appears not to have been set, assign the test to IS NULL
                $k .= ' IS NULL';
            } elseif (preg_match('/\s*(!?=|<>|\sIS(?:\s+NOT)?\s)\s*$/i', $k, $match, PREG_OFFSET_CAPTURE)) {
                $k = substr($k, 0, $match[0][1]) . ($match[1][0] === '=' ? ' IS NULL' : ' IS NOT NULL');
            }

            ${$qb_key} = array('condition' => $prefix . $k, 'value' => $v, 'escape' => $escape);
            $this->{$qb_key}[] = ${$qb_key};
            if ($this->qb_caching === TRUE) {
                $this->{$qb_cache_key}[] = ${$qb_key};
                $this->qb_cache_exists[] = substr($qb_key, 3);
            }

        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * WHERE IN
     *
     * Generates a WHERE field IN('item', 'item') SQL query,
     * joined with 'AND' if appropriate.
     *
     * @param    string $key The field to search
     * @param    array $values The values searched on
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function where_in($key = NULL, $values = NULL, $escape = NULL)
    {
        return $this->_where_in($key, $values, FALSE, 'AND ', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * OR WHERE IN
     *
     * Generates a WHERE field IN('item', 'item') SQL query,
     * joined with 'OR' if appropriate.
     *
     * @param    string $key The field to search
     * @param    array $values The values searched on
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function or_where_in($key = NULL, $values = NULL, $escape = NULL)
    {
        return $this->_where_in($key, $values, FALSE, 'OR ', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * WHERE NOT IN
     *
     * Generates a WHERE field NOT IN('item', 'item') SQL query,
     * joined with 'AND' if appropriate.
     *
     * @param    string $key The field to search
     * @param    array $values The values searched on
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function where_not_in($key = NULL, $values = NULL, $escape = NULL)
    {
        return $this->_where_in($key, $values, TRUE, 'AND ', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * OR WHERE NOT IN
     *
     * Generates a WHERE field NOT IN('item', 'item') SQL query,
     * joined with 'OR' if appropriate.
     *
     * @param    string $key The field to search
     * @param    array $values The values searched on
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function or_where_not_in($key = NULL, $values = NULL, $escape = NULL)
    {
        return $this->_where_in($key, $values, TRUE, 'OR ', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * Internal WHERE IN
     *
     * @used-by    where_in()
     * @used-by    or_where_in()
     * @used-by    where_not_in()
     * @used-by    or_where_not_in()
     *
     * @param    string $key The field to search
     * @param    array $values The values searched on
     * @param    bool $not If the statement would be IN or NOT IN
     * @param    string $type
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    protected function _where_in($key = NULL, $values = NULL, $not = FALSE, $type = 'AND ', $escape = NULL)
    {
        if ($key === NULL OR $values === NULL) {
            return $this;
        }

        if (!is_array($values)) {
            $values = array($values);
        }

        is_bool($escape) OR $escape = $this->_protect_identifiers;

        $not = ($not) ? ' NOT' : '';

        if ($escape === TRUE) {
            $where_in = array();
            foreach ($values as $value) {
                $where_in[] = $this->escape($value);
            }
        } else {
            $where_in = array_values($values);
        }

        $prefix = (count($this->qb_where) === 0 && count($this->qb_cache_where) === 0)
            ? $this->_group_get_type('')
            : $this->_group_get_type($type);

        $where_in = array(
            'condition' => $prefix . $key . $not . ' IN(' . implode(', ', $where_in) . ')',
            'value' => NULL,
            'escape' => $escape
        );

        $this->qb_where[] = $where_in;
        if ($this->qb_caching === TRUE) {
            $this->qb_cache_where[] = $where_in;
            $this->qb_cache_exists[] = 'where';
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * LIKE
     *
     * Generates a %LIKE% portion of the query.
     * Separates multiple calls with 'AND'.
     *
     * @param    mixed $field
     * @param    string $match
     * @param    string $side
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function like($field, $match = '', $side = 'both', $escape = NULL)
    {
        return $this->_like($field, $match, 'AND ', $side, '', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * NOT LIKE
     *
     * Generates a NOT LIKE portion of the query.
     * Separates multiple calls with 'AND'.
     *
     * @param    mixed $field
     * @param    string $match
     * @param    string $side
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function not_like($field, $match = '', $side = 'both', $escape = NULL)
    {
        return $this->_like($field, $match, 'AND ', $side, 'NOT', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * OR LIKE
     *
     * Generates a %LIKE% portion of the query.
     * Separates multiple calls with 'OR'.
     *
     * @param    mixed $field
     * @param    string $match
     * @param    string $side
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function or_like($field, $match = '', $side = 'both', $escape = NULL)
    {
        return $this->_like($field, $match, 'OR ', $side, '', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * OR NOT LIKE
     *
     * Generates a NOT LIKE portion of the query.
     * Separates multiple calls with 'OR'.
     *
     * @param    mixed $field
     * @param    string $match
     * @param    string $side
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function or_not_like($field, $match = '', $side = 'both', $escape = NULL)
    {
        return $this->_like($field, $match, 'OR ', $side, 'NOT', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * Internal LIKE
     *
     * @used-by    like()
     * @used-by    or_like()
     * @used-by    not_like()
     * @used-by    or_not_like()
     *
     * @param    mixed $field
     * @param    string $match
     * @param    string $type
     * @param    string $side
     * @param    string $not
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    protected function _like($field, $match = '', $type = 'AND ', $side = 'both', $not = '', $escape = NULL)
    {
        if (!is_array($field)) {
            $field = array($field => $match);
        }

        is_bool($escape) OR $escape = $this->_protect_identifiers;
        // lowercase $side in case somebody writes e.g. 'BEFORE' instead of 'before' (doh)
        $side = strtolower($side);

        foreach ($field as $k => $v) {
            $prefix = (count($this->qb_where) === 0 && count($this->qb_cache_where) === 0)
                ? $this->_group_get_type('') : $this->_group_get_type($type);

            if ($escape === TRUE) {
                $v = $this->escape_like_str($v);
            }

            switch ($side) {
                case 'none':
                    $v = "'{$v}'";
                    break;
                case 'before':
                    $v = "'%{$v}'";
                    break;
                case 'after':
                    $v = "'{$v}%'";
                    break;
                case 'both':
                default:
                    $v = "'%{$v}%'";
                    break;
            }

            // some platforms require an escape sequence definition for LIKE wildcards
            if ($escape === TRUE && $this->_like_escape_str !== '') {
                $v .= sprintf($this->_like_escape_str, $this->_like_escape_chr);
            }

            $qb_where = array('condition' => "{$prefix} {$k} {$not} LIKE {$v}", 'value' => NULL, 'escape' => $escape);
            $this->qb_where[] = $qb_where;
            if ($this->qb_caching === TRUE) {
                $this->qb_cache_where[] = $qb_where;
                $this->qb_cache_exists[] = 'where';
            }
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Starts a query group.
     *
     * @param    string $not (Internal use only)
     * @param    string $type (Internal use only)
     * @return    CI_DB_query_builder
     */
    public function group_start($not = '', $type = 'AND ')
    {
        $type = $this->_group_get_type($type);

        $this->qb_where_group_started = TRUE;
        $prefix = (count($this->qb_where) === 0 && count($this->qb_cache_where) === 0) ? '' : $type;
        $where = array(
            'condition' => $prefix . $not . str_repeat(' ', ++$this->qb_where_group_count) . ' (',
            'value' => NULL,
            'escape' => FALSE
        );

        $this->qb_where[] = $where;
        if ($this->qb_caching) {
            $this->qb_cache_where[] = $where;
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Starts a query group, but ORs the group
     *
     * @return    CI_DB_query_builder
     */
    public function or_group_start()
    {
        return $this->group_start('', 'OR ');
    }

    // --------------------------------------------------------------------

    /**
     * Starts a query group, but NOTs the group
     *
     * @return    CI_DB_query_builder
     */
    public function not_group_start()
    {
        return $this->group_start('NOT ', 'AND ');
    }

    // --------------------------------------------------------------------

    /**
     * Starts a query group, but OR NOTs the group
     *
     * @return    CI_DB_query_builder
     */
    public function or_not_group_start()
    {
        return $this->group_start('NOT ', 'OR ');
    }

    // --------------------------------------------------------------------

    /**
     * Ends a query group
     *
     * @return    CI_DB_query_builder
     */
    public function group_end()
    {
        $this->qb_where_group_started = FALSE;
        $where = array(
            'condition' => str_repeat(' ', $this->qb_where_group_count--) . ')',
            'value' => NULL,
            'escape' => FALSE
        );

        $this->qb_where[] = $where;
        if ($this->qb_caching) {
            $this->qb_cache_where[] = $where;
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Group_get_type
     *
     * @used-by    group_start()
     * @used-by    _like()
     * @used-by    _wh()
     * @used-by    _where_in()
     *
     * @param    string $type
     * @return    string
     */
    protected function _group_get_type($type)
    {
        if ($this->qb_where_group_started) {
            $type = '';
            $this->qb_where_group_started = FALSE;
        }

        return $type;
    }

    // --------------------------------------------------------------------

    /**
     * GROUP BY
     *
     * @param    string $by
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function group_by($by, $escape = NULL)
    {
        is_bool($escape) OR $escape = $this->_protect_identifiers;

        if (is_string($by)) {
            $by = ($escape === TRUE)
                ? explode(',', $by)
                : array($by);
        }

        foreach ($by as $val) {
            $val = trim($val);

            if ($val !== '') {
                $val = array('field' => $val, 'escape' => $escape);

                $this->qb_groupby[] = $val;
                if ($this->qb_caching === TRUE) {
                    $this->qb_cache_groupby[] = $val;
                    $this->qb_cache_exists[] = 'groupby';
                }
            }
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * HAVING
     *
     * Separates multiple calls with 'AND'.
     *
     * @param    string $key
     * @param    string $value
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function having($key, $value = NULL, $escape = NULL)
    {
        return $this->_wh('qb_having', $key, $value, 'AND ', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * OR HAVING
     *
     * Separates multiple calls with 'OR'.
     *
     * @param    string $key
     * @param    string $value
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function or_having($key, $value = NULL, $escape = NULL)
    {
        return $this->_wh('qb_having', $key, $value, 'OR ', $escape);
    }

    // --------------------------------------------------------------------

    /**
     * ORDER BY
     *
     * @param    string $orderby
     * @param    string $direction ASC, DESC or RANDOM
     * @param    bool $escape
     * @return    CI_DB_query_builder
     */
    public function order_by($orderby, $direction = '', $escape = NULL)
    {
        $direction = strtoupper(trim($direction));

        if ($direction === 'RANDOM') {
            $direction = '';

            // Do we have a seed value?
            $orderby = ctype_digit((string)$orderby)
                ? sprintf($this->_random_keyword[1], $orderby)
                : $this->_random_keyword[0];
        } elseif (empty($orderby)) {
            return $this;
        } elseif ($direction !== '') {
            $direction = in_array($direction, array('ASC', 'DESC'), TRUE) ? ' ' . $direction : '';
        }

        is_bool($escape) OR $escape = $this->_protect_identifiers;

        if ($escape === FALSE) {
            $qb_orderby[] = array('field' => $orderby, 'direction' => $direction, 'escape' => FALSE);
        } else {
            $qb_orderby = array();
            foreach (explode(',', $orderby) as $field) {
                $qb_orderby[] = ($direction === '' && preg_match('/\s+(ASC|DESC)$/i', rtrim($field), $match, PREG_OFFSET_CAPTURE))
                    ? array('field' => ltrim(substr($field, 0, $match[0][1])), 'direction' => ' ' . $match[1][0], 'escape' => TRUE)
                    : array('field' => trim($field), 'direction' => $direction, 'escape' => TRUE);
            }
        }

        $this->qb_orderby = array_merge($this->qb_orderby, $qb_orderby);
        if ($this->qb_caching === TRUE) {
            $this->qb_cache_orderby = array_merge($this->qb_cache_orderby, $qb_orderby);
            $this->qb_cache_exists[] = 'orderby';
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * LIMIT
     *
     * @param    int $value LIMIT value
     * @param    int $offset OFFSET value
     * @return    CI_DB_query_builder
     */
    public function limit($value, $offset = 0)
    {
        is_null($value) OR $this->qb_limit = (int)$value;
        empty($offset) OR $this->qb_offset = (int)$offset;

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Sets the OFFSET value
     *
     * @param    int $offset OFFSET value
     * @return    CI_DB_query_builder
     */
    public function offset($offset)
    {
        empty($offset) OR $this->qb_offset = (int)$offset;
        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * LIMIT string
     *
     * Generates a platform-specific LIMIT clause.
     *
     * @param    string $sql SQL Query
     * @return    string
     */
    protected function _limit($sql)
    {
        return $sql . ' LIMIT ' . ($this->qb_offset ? $this->qb_offset . ', ' : '') . (int)$this->qb_limit;
    }

    // --------------------------------------------------------------------

    /**
     * The "set" function.
     *
     * Allows key/value pairs to be set for inserting or updating
     *
     * @param    mixed
     * @param    string
     * @param    bool
     * @return    CI_DB_query_builder
     */
    public function set($key, $value = '', $escape = NULL)
    {
        $key = $this->_object_to_array($key);

        if (!is_array($key)) {
            $key = array($key => $value);
        }

        is_bool($escape) OR $escape = $this->_protect_identifiers;

        foreach ($key as $k => $v) {
            $this->qb_set[$this->protect_identifiers($k, FALSE, $escape)] = ($escape)
                ? $this->escape($v) : $v;
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Get SELECT query string
     *
     * Compiles a SELECT query string and returns the sql.
     *
     * @param    string    the table name to select from (optional)
     * @param    bool    TRUE: resets QB values; FALSE: leave QB values alone
     * @return    string
     */
    public function get_compiled_select($table = '', $reset = TRUE)
    {
        if ($table !== '') {
            $this->_track_aliases($table);
            $this->from($table);
        }

        $select = $this->_compile_select();

        if ($reset === TRUE) {
            $this->_reset_select();
        }

        return $select;
    }

    // --------------------------------------------------------------------

    /**
     * Get
     *
     * Compiles the select statement based on the other functions called
     * and runs the query
     *
     * @param    string    the table
     * @param    string    the limit clause
     * @param    string    the offset clause
     * @return    CI_DB_result
     */
    public function get($table = '', $limit = NULL, $offset = NULL)
    {
        if ($table !== '') {
            $this->_track_aliases($table);
            $this->from($table);
        }

        if (!empty($limit)) {
            $this->limit($limit, $offset);
        }

        $result = $this->query($this->_compile_select());
        $this->_reset_select();
        return $result;
    }

    // --------------------------------------------------------------------

    /**
     * "Count All Results" query
     *
     * Generates a platform-specific query string that counts all records
     * returned by an Query Builder query.
     *
     * @param    string
     * @param    bool    the reset clause
     * @return    int
     */
    public function count_all_results($table = '', $reset = TRUE)
    {
        if ($table !== '') {
            $this->_track_aliases($table);
            $this->from($table);
        }

        // ORDER BY usage is often problematic here (most notably
        // on Microsoft SQL Server) and ultimately unnecessary
        // for selecting COUNT(*) ...
        $qb_orderby = $this->qb_orderby;
        $qb_cache_orderby = $this->qb_cache_orderby;
        $this->qb_orderby = $this->qb_cache_orderby = array();

        $result = ($this->qb_distinct === TRUE OR !empty($this->qb_groupby) OR !empty($this->qb_cache_groupby) OR $this->qb_limit OR $this->qb_offset)
            ? $this->query($this->_count_string . $this->protect_identifiers('numrows') . "\nFROM (\n" . $this->_compile_select() . "\n) CI_count_all_results")
            : $this->query($this->_compile_select($this->_count_string . $this->protect_identifiers('numrows')));

        if ($reset === TRUE) {
            $this->_reset_select();
        } else {
            $this->qb_orderby = $qb_orderby;
            $this->qb_cache_orderby = $qb_cache_orderby;
        }

        if ($result->num_rows() === 0) {
            return 0;
        }

        $row = $result->row();
        return (int)$row->numrows;
    }

    // --------------------------------------------------------------------

    /**
     * get_where()
     *
     * Allows the where clause, limit and offset to be added directly
     *
     * @param    string $table
     * @param    string $where
     * @param    int $limit
     * @param    int $offset
     * @return    CI_DB_result
     */
    public function get_where($table = '', $where = NULL, $limit = NULL, $offset = NULL)
    {
        if ($table !== '') {
            $this->from($table);
        }

        if ($where !== NULL) {
            $this->where($where);
        }

        if (!empty($limit)) {
            $this->limit($limit, $offset);
        }

        $result = $this->query($this->_compile_select());
        $this->_reset_select();
        return $result;
    }

    // --------------------------------------------------------------------

    /**
     * Insert_Batch
     *
     * Compiles batch insert strings and runs the queries
     *
     * @param    string $table Table to insert into
     * @param    array $set An associative array of insert values
     * @param    bool $escape Whether to escape values and identifiers
     * @return    int    Number of rows inserted or FALSE on failure
     */
    public function insert_batch($table, $set = NULL, $escape = NULL, $batch_size = 100)
    {
        if ($set === NULL) {
            if (empty($this->qb_set)) {
                return ($this->db_debug) ? $this->display_error('db_must_use_set') : FALSE;
            }
        } else {
            if (empty($set)) {
                return ($this->db_debug) ? $this->display_error('insert_batch() called with no data') : FALSE;
            }

            $this->set_insert_batch($set, '', $escape);
        }

        if (strlen($table) === 0) {
            if (!isset($this->qb_from[0])) {
                return ($this->db_debug) ? $this->display_error('db_must_set_table') : FALSE;
            }

            $table = $this->qb_from[0];
        }

        // Batch this baby
        $affected_rows = 0;
        for ($i = 0, $total = count($this->qb_set); $i < $total; $i += $batch_size) {
            if ($this->query($this->_insert_batch($this->protect_identifiers($table, TRUE, $escape, FALSE), $this->qb_keys, array_slice($this->qb_set, $i, $batch_size)))) {
                $affected_rows += $this->affected_rows();
            }
        }

        $this->_reset_write();
        return $affected_rows;
    }

    // --------------------------------------------------------------------

    /**
     * Insert batch statement
     *
     * Generates a platform-specific insert string from the supplied data.
     *
     * @param    string $table Table name
     * @param    array $keys INSERT keys
     * @param    array $values INSERT values
     * @return    string
     */
    protected function _insert_batch($table, $keys, $values)
    {
        return 'INSERT INTO ' . $table . ' (' . implode(', ', $keys) . ') VALUES ' . implode(', ', $values);
    }

    // --------------------------------------------------------------------

    /**
     * The "set_insert_batch" function.  Allows key/value pairs to be set for batch inserts
     *
     * @param    mixed
     * @param    string
     * @param    bool
     * @return    CI_DB_query_builder
     */
    public function set_insert_batch($key, $value = '', $escape = NULL)
    {
        $key = $this->_object_to_array_batch($key);

        if (!is_array($key)) {
            $key = array($key => $value);
        }

        is_bool($escape) OR $escape = $this->_protect_identifiers;

        $keys = array_keys($this->_object_to_array(reset($key)));
        sort($keys);

        foreach ($key as $row) {
            $row = $this->_object_to_array($row);
            if (count(array_diff($keys, array_keys($row))) > 0 OR count(array_diff(array_keys($row), $keys)) > 0) {
                // batch function above returns an error on an empty array
                $this->qb_set[] = array();
                return;
            }

            ksort($row); // puts $row in the same order as our keys

            if ($escape !== FALSE) {
                $clean = array();
                foreach ($row as $value) {
                    $clean[] = $this->escape($value);
                }

                $row = $clean;
            }

            $this->qb_set[] = '(' . implode(',', $row) . ')';
        }

        foreach ($keys as $k) {
            $this->qb_keys[] = $this->protect_identifiers($k, FALSE, $escape);
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Get INSERT query string
     *
     * Compiles an insert query and returns the sql
     *
     * @param    string    the table to insert into
     * @param    bool    TRUE: reset QB values; FALSE: leave QB values alone
     * @return    string
     */
    public function get_compiled_insert($table = '', $reset = TRUE)
    {
        if ($this->_validate_insert($table) === FALSE) {
            return FALSE;
        }

        $sql = $this->_insert(
            $this->protect_identifiers(
                $this->qb_from[0], TRUE, NULL, FALSE
            ),
            array_keys($this->qb_set),
            array_values($this->qb_set)
        );

        if ($reset === TRUE) {
            $this->_reset_write();
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Insert
     *
     * Compiles an insert string and runs the query
     *
     * @param    string    the table to insert data into
     * @param    array    an associative array of insert values
     * @param    bool $escape Whether to escape values and identifiers
     * @return    bool    TRUE on success, FALSE on failure
     */
    public function insert($table = '', $set = NULL, $escape = NULL)
    {
        if ($set !== NULL) {
            $this->set($set, '', $escape);
        }

        if ($this->_validate_insert($table) === FALSE) {
            return FALSE;
        }

        $sql = $this->_insert(
            $this->protect_identifiers(
                $this->qb_from[0], TRUE, $escape, FALSE
            ),
            array_keys($this->qb_set),
            array_values($this->qb_set)
        );

        $this->_reset_write();
        return $this->query($sql);
    }

    // --------------------------------------------------------------------

    /**
     * Validate Insert
     *
     * This method is used by both insert() and get_compiled_insert() to
     * validate that the there data is actually being set and that table
     * has been chosen to be inserted into.
     *
     * @param    string    the table to insert data into
     * @return    string
     */
    protected function _validate_insert($table = '')
    {
        if (count($this->qb_set) === 0) {
            return ($this->db_debug) ? $this->display_error('db_must_use_set') : FALSE;
        }

        if ($table !== '') {
            $this->qb_from[0] = $table;
        } elseif (!isset($this->qb_from[0])) {
            return ($this->db_debug) ? $this->display_error('db_must_set_table') : FALSE;
        }

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Replace
     *
     * Compiles an replace into string and runs the query
     *
     * @param    string    the table to replace data into
     * @param    array    an associative array of insert values
     * @return    bool    TRUE on success, FALSE on failure
     */
    public function replace($table = '', $set = NULL)
    {
        if ($set !== NULL) {
            $this->set($set);
        }

        if (count($this->qb_set) === 0) {
            return ($this->db_debug) ? $this->display_error('db_must_use_set') : FALSE;
        }

        if ($table === '') {
            if (!isset($this->qb_from[0])) {
                return ($this->db_debug) ? $this->display_error('db_must_set_table') : FALSE;
            }

            $table = $this->qb_from[0];
        }

        $sql = $this->_replace($this->protect_identifiers($table, TRUE, NULL, FALSE), array_keys($this->qb_set), array_values($this->qb_set));

        $this->_reset_write();
        return $this->query($sql);
    }

    // --------------------------------------------------------------------

    /**
     * Replace statement
     *
     * Generates a platform-specific replace string from the supplied data
     *
     * @param    string    the table name
     * @param    array    the insert keys
     * @param    array    the insert values
     * @return    string
     */
    protected function _replace($table, $keys, $values)
    {
        return 'REPLACE INTO ' . $table . ' (' . implode(', ', $keys) . ') VALUES (' . implode(', ', $values) . ')';
    }

    // --------------------------------------------------------------------

    /**
     * FROM tables
     *
     * Groups tables in FROM clauses if needed, so there is no confusion
     * about operator precedence.
     *
     * Note: This is only used (and overridden) by MySQL and CUBRID.
     *
     * @return    string
     */
    protected function _from_tables()
    {
        return implode(', ', $this->qb_from);
    }

    // --------------------------------------------------------------------

    /**
     * Get UPDATE query string
     *
     * Compiles an update query and returns the sql
     *
     * @param    string    the table to update
     * @param    bool    TRUE: reset QB values; FALSE: leave QB values alone
     * @return    string
     */
    public function get_compiled_update($table = '', $reset = TRUE)
    {
        // Combine any cached components with the current statements
        $this->_merge_cache();

        if ($this->_validate_update($table) === FALSE) {
            return FALSE;
        }

        $sql = $this->_update($this->qb_from[0], $this->qb_set);

        if ($reset === TRUE) {
            $this->_reset_write();
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * UPDATE
     *
     * Compiles an update string and runs the query.
     *
     * @param    string $table
     * @param    array $set An associative array of update values
     * @param    mixed $where
     * @param    int $limit
     * @return    bool    TRUE on success, FALSE on failure
     */
    public function update($table = '', $set = NULL, $where = NULL, $limit = NULL)
    {
        // Combine any cached components with the current statements
        $this->_merge_cache();

        if ($set !== NULL) {
            $this->set($set);
        }

        if ($this->_validate_update($table) === FALSE) {
            return FALSE;
        }

        if ($where !== NULL) {
            $this->where($where);
        }

        if (!empty($limit)) {
            $this->limit($limit);
        }

        $sql = $this->_update($this->qb_from[0], $this->qb_set);
        $this->_reset_write();
        return $this->query($sql);
    }

    // --------------------------------------------------------------------

    /**
     * Validate Update
     *
     * This method is used by both update() and get_compiled_update() to
     * validate that data is actually being set and that a table has been
     * chosen to be update.
     *
     * @param    string    the table to update data on
     * @return    bool
     */
    protected function _validate_update($table)
    {
        if (count($this->qb_set) === 0) {
            return ($this->db_debug) ? $this->display_error('db_must_use_set') : FALSE;
        }

        if ($table !== '') {
            $this->qb_from = array($this->protect_identifiers($table, TRUE, NULL, FALSE));
        } elseif (!isset($this->qb_from[0])) {
            return ($this->db_debug) ? $this->display_error('db_must_set_table') : FALSE;
        }

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Update_Batch
     *
     * Compiles an update string and runs the query
     *
     * @param    string    the table to retrieve the results from
     * @param    array    an associative array of update values
     * @param    string    the where key
     * @return    int    number of rows affected or FALSE on failure
     */
    public function update_batch($table, $set = NULL, $index = NULL, $batch_size = 100)
    {
        // Combine any cached components with the current statements
        $this->_merge_cache();

        if ($index === NULL) {
            return ($this->db_debug) ? $this->display_error('db_must_use_index') : FALSE;
        }

        if ($set === NULL) {
            if (empty($this->qb_set_ub)) {
                return ($this->db_debug) ? $this->display_error('db_must_use_set') : FALSE;
            }
        } else {
            if (empty($set)) {
                return ($this->db_debug) ? $this->display_error('update_batch() called with no data') : FALSE;
            }

            $this->set_update_batch($set, $index);
        }

        if (strlen($table) === 0) {
            if (!isset($this->qb_from[0])) {
                return ($this->db_debug) ? $this->display_error('db_must_set_table') : FALSE;
            }

            $table = $this->qb_from[0];
        }

        // Batch this baby
        $affected_rows = 0;
        for ($i = 0, $total = count($this->qb_set_ub); $i < $total; $i += $batch_size) {
            if ($this->query($this->_update_batch($this->protect_identifiers($table, TRUE, NULL, FALSE), array_slice($this->qb_set_ub, $i, $batch_size), $index))) {
                $affected_rows += $this->affected_rows();
            }

            $this->qb_where = array();
        }

        $this->_reset_write();
        return $affected_rows;
    }

    // --------------------------------------------------------------------

    /**
     * Update_Batch statement
     *
     * Generates a platform-specific batch update string from the supplied data
     *
     * @param    string $table Table name
     * @param    array $values Update data
     * @param    string $index WHERE key
     * @return    string
     */
    protected function _update_batch($table, $values, $index)
    {
        $ids = array();
        foreach ($values as $key => $val) {
            $ids[] = $val[$index]['value'];

            foreach (array_keys($val) as $field) {
                if ($field !== $index) {
                    $final[$val[$field]['field']][] = 'WHEN ' . $val[$index]['field'] . ' = ' . $val[$index]['value'] . ' THEN ' . $val[$field]['value'];
                }
            }
        }

        $cases = '';
        foreach ($final as $k => $v) {
            $cases .= $k . " = CASE \n"
                . implode("\n", $v) . "\n"
                . 'ELSE ' . $k . ' END, ';
        }

        $this->where($val[$index]['field'] . ' IN(' . implode(',', $ids) . ')', NULL, FALSE);

        return 'UPDATE ' . $table . ' SET ' . substr($cases, 0, -2) . $this->_compile_wh('qb_where');
    }

    // --------------------------------------------------------------------

    /**
     * The "set_update_batch" function.  Allows key/value pairs to be set for batch updating
     *
     * @param    array
     * @param    string
     * @param    bool
     * @return    CI_DB_query_builder
     */
    public function set_update_batch($key, $index = '', $escape = NULL)
    {
        $key = $this->_object_to_array_batch($key);

        if (!is_array($key)) {
            // @todo error
        }

        is_bool($escape) OR $escape = $this->_protect_identifiers;

        foreach ($key as $k => $v) {
            $index_set = FALSE;
            $clean = array();
            foreach ($v as $k2 => $v2) {
                if ($k2 === $index) {
                    $index_set = TRUE;
                }

                $clean[$k2] = array(
                    'field' => $this->protect_identifiers($k2, FALSE, $escape),
                    'value' => ($escape === FALSE ? $v2 : $this->escape($v2))
                );
            }

            if ($index_set === FALSE) {
                return $this->display_error('db_batch_missing_index');
            }

            $this->qb_set_ub[] = $clean;
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Empty Table
     *
     * Compiles a delete string and runs "DELETE FROM table"
     *
     * @param    string    the table to empty
     * @return    bool    TRUE on success, FALSE on failure
     */
    public function empty_table($table = '')
    {
        if ($table === '') {
            if (!isset($this->qb_from[0])) {
                return ($this->db_debug) ? $this->display_error('db_must_set_table') : FALSE;
            }

            $table = $this->qb_from[0];
        } else {
            $table = $this->protect_identifiers($table, TRUE, NULL, FALSE);
        }

        $sql = $this->_delete($table);
        $this->_reset_write();
        return $this->query($sql);
    }

    // --------------------------------------------------------------------

    /**
     * Truncate
     *
     * Compiles a truncate string and runs the query
     * If the database does not support the truncate() command
     * This function maps to "DELETE FROM table"
     *
     * @param    string    the table to truncate
     * @return    bool    TRUE on success, FALSE on failure
     */
    public function truncate($table = '')
    {
        if ($table === '') {
            if (!isset($this->qb_from[0])) {
                return ($this->db_debug) ? $this->display_error('db_must_set_table') : FALSE;
            }

            $table = $this->qb_from[0];
        } else {
            $table = $this->protect_identifiers($table, TRUE, NULL, FALSE);
        }

        $sql = $this->_truncate($table);
        $this->_reset_write();
        return $this->query($sql);
    }

    // --------------------------------------------------------------------

    /**
     * Truncate statement
     *
     * Generates a platform-specific truncate string from the supplied data
     *
     * If the database does not support the truncate() command,
     * then this method maps to 'DELETE FROM table'
     *
     * @param    string    the table name
     * @return    string
     */
    protected function _truncate($table)
    {
        return 'TRUNCATE ' . $table;
    }

    // --------------------------------------------------------------------

    /**
     * Get DELETE query string
     *
     * Compiles a delete query string and returns the sql
     *
     * @param    string    the table to delete from
     * @param    bool    TRUE: reset QB values; FALSE: leave QB values alone
     * @return    string
     */
    public function get_compiled_delete($table = '', $reset = TRUE)
    {
        $this->return_delete_sql = TRUE;
        $sql = $this->delete($table, '', NULL, $reset);
        $this->return_delete_sql = FALSE;
        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Delete
     *
     * Compiles a delete string and runs the query
     *
     * @param    mixed    the table(s) to delete from. String or array
     * @param    mixed    the where clause
     * @param    mixed    the limit clause
     * @param    bool
     * @return    mixed
     */
    public function delete($table = '', $where = '', $limit = NULL, $reset_data = TRUE)
    {
        // Combine any cached components with the current statements
        $this->_merge_cache();

        if ($table === '') {
            if (!isset($this->qb_from[0])) {
                return ($this->db_debug) ? $this->display_error('db_must_set_table') : FALSE;
            }

            $table = $this->qb_from[0];
        } elseif (is_array($table)) {
            empty($where) && $reset_data = FALSE;

            foreach ($table as $single_table) {
                $this->delete($single_table, $where, $limit, $reset_data);
            }

            return;
        } else {
            $table = $this->protect_identifiers($table, TRUE, NULL, FALSE);
        }

        if ($where !== '') {
            $this->where($where);
        }

        if (!empty($limit)) {
            $this->limit($limit);
        }

        if (count($this->qb_where) === 0) {
            return ($this->db_debug) ? $this->display_error('db_del_must_use_where') : FALSE;
        }

        $sql = $this->_delete($table);
        if ($reset_data) {
            $this->_reset_write();
        }

        return ($this->return_delete_sql === TRUE) ? $sql : $this->query($sql);
    }

    // --------------------------------------------------------------------

    /**
     * Delete statement
     *
     * Generates a platform-specific delete string from the supplied data
     *
     * @param    string    the table name
     * @return    string
     */
    protected function _delete($table)
    {
        return 'DELETE FROM ' . $table . $this->_compile_wh('qb_where')
            . ($this->qb_limit !== FALSE ? ' LIMIT ' . $this->qb_limit : '');
    }

    // --------------------------------------------------------------------

    /**
     * DB Prefix
     *
     * Prepends a database prefix if one exists in configuration
     *
     * @param    string    the table
     * @return    string
     */
    public function dbprefix($table = '')
    {
        if ($table === '') {
            $this->display_error('db_table_name_required');
        }

        return $this->dbprefix . $table;
    }

    // --------------------------------------------------------------------

    /**
     * Set DB Prefix
     *
     * Set's the DB Prefix to something new without needing to reconnect
     *
     * @param    string    the prefix
     * @return    string
     */
    public function set_dbprefix($prefix = '')
    {
        return $this->dbprefix = $prefix;
    }

    // --------------------------------------------------------------------

    /**
     * Track Aliases
     *
     * Used to track SQL statements written with aliased tables.
     *
     * @param    string    The table to inspect
     * @return    string
     */
    protected function _track_aliases($table)
    {
        if (is_array($table)) {
            foreach ($table as $t) {
                $this->_track_aliases($t);
            }
            return;
        }

        // Does the string contain a comma?  If so, we need to separate
        // the string into discreet statements
        if (strpos($table, ',') !== FALSE) {
            return $this->_track_aliases(explode(',', $table));
        }

        // if a table alias is used we can recognize it by a space
        if (strpos($table, ' ') !== FALSE) {
            // if the alias is written with the AS keyword, remove it
            $table = preg_replace('/\s+AS\s+/i', ' ', $table);

            // Grab the alias
            $table = trim(strrchr($table, ' '));

            // Store the alias, if it doesn't already exist
            if (!in_array($table, $this->qb_aliased_tables, TRUE)) {
                $this->qb_aliased_tables[] = $table;
                if ($this->qb_caching === TRUE && !in_array($table, $this->qb_cache_aliased_tables, TRUE)) {
                    $this->qb_cache_aliased_tables[] = $table;
                    $this->qb_cache_exists[] = 'aliased_tables';
                }
            }
        }
    }

    // --------------------------------------------------------------------

    /**
     * Compile the SELECT statement
     *
     * Generates a query string based on which functions were used.
     * Should not be called directly.
     *
     * @param    bool $select_override
     * @return    string
     */
    protected function _compile_select($select_override = FALSE)
    {
        // Combine any cached components with the current statements
        $this->_merge_cache();

        // Write the "select" portion of the query
        if ($select_override !== FALSE) {
            $sql = $select_override;
        } else {
            $sql = (!$this->qb_distinct) ? 'SELECT ' : 'SELECT DISTINCT ';

            if (count($this->qb_select) === 0) {
                $sql .= '*';
            } else {
                // Cycle through the "select" portion of the query and prep each column name.
                // The reason we protect identifiers here rather than in the select() function
                // is because until the user calls the from() function we don't know if there are aliases
                foreach ($this->qb_select as $key => $val) {
                    $no_escape = isset($this->qb_no_escape[$key]) ? $this->qb_no_escape[$key] : NULL;
                    $this->qb_select[$key] = $this->protect_identifiers($val, FALSE, $no_escape);
                }

                $sql .= implode(', ', $this->qb_select);
            }
        }

        // Write the "FROM" portion of the query
        if (count($this->qb_from) > 0) {
            $sql .= "\nFROM " . $this->_from_tables();
        }

        // Write the "JOIN" portion of the query
        if (count($this->qb_join) > 0) {
            $sql .= "\n" . implode("\n", $this->qb_join);
        }

        $sql .= $this->_compile_wh('qb_where')
            . $this->_compile_group_by()
            . $this->_compile_wh('qb_having')
            . $this->_compile_order_by(); // ORDER BY

        // LIMIT
        if ($this->qb_limit !== FALSE OR $this->qb_offset) {
            return $this->_limit($sql . "\n");
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Compile WHERE, HAVING statements
     *
     * Escapes identifiers in WHERE and HAVING statements at execution time.
     *
     * Required so that aliases are tracked properly, regardless of whether
     * where(), or_where(), having(), or_having are called prior to from(),
     * join() and dbprefix is added only if needed.
     *
     * @param    string $qb_key 'qb_where' or 'qb_having'
     * @return    string    SQL statement
     */
    protected function _compile_wh($qb_key)
    {
        if (count($this->$qb_key) > 0) {
            for ($i = 0, $c = count($this->$qb_key); $i < $c; $i++) {
                // Is this condition already compiled?
                if (is_string($this->{$qb_key}[$i])) {
                    continue;
                } elseif ($this->{$qb_key}[$i]['escape'] === FALSE) {
                    $this->{$qb_key}[$i] = $this->{$qb_key}[$i]['condition'] . (isset($this->{$qb_key}[$i]['value']) ? ' ' . $this->{$qb_key}[$i]['value'] : '');
                    continue;
                }

                // Split multiple conditions
                $conditions = preg_split(
                    '/((?:^|\s+)AND\s+|(?:^|\s+)OR\s+)/i',
                    $this->{$qb_key}[$i]['condition'],
                    -1,
                    PREG_SPLIT_DELIM_CAPTURE | PREG_SPLIT_NO_EMPTY
                );

                for ($ci = 0, $cc = count($conditions); $ci < $cc; $ci++) {
                    if (($op = $this->_get_operator($conditions[$ci])) === FALSE
                        OR !preg_match('/^(\(?)(.*)(' . preg_quote($op, '/') . ')\s*(.*(?<!\)))?(\)?)$/i', $conditions[$ci], $matches)
                    ) {
                        continue;
                    }

                    // $matches = array(
                    //	0 => '(test <= foo)',	/* the whole thing */
                    //	1 => '(',		/* optional */
                    //	2 => 'test',		/* the field name */
                    //	3 => ' <= ',		/* $op */
                    //	4 => 'foo',		/* optional, if $op is e.g. 'IS NULL' */
                    //	5 => ')'		/* optional */
                    // );

                    if (!empty($matches[4])) {
                        $this->_is_literal($matches[4]) OR $matches[4] = $this->protect_identifiers(trim($matches[4]));
                        $matches[4] = ' ' . $matches[4];
                    }

                    $conditions[$ci] = $matches[1] . $this->protect_identifiers(trim($matches[2]))
                        . ' ' . trim($matches[3]) . $matches[4] . $matches[5];
                }

                $this->{$qb_key}[$i] = implode('', $conditions) . (isset($this->{$qb_key}[$i]['value']) ? ' ' . $this->{$qb_key}[$i]['value'] : '');
            }

            return ($qb_key === 'qb_having' ? "\nHAVING " : "\nWHERE ")
                . implode("\n", $this->$qb_key);
        }

        return '';
    }

    // --------------------------------------------------------------------

    /**
     * Compile GROUP BY
     *
     * Escapes identifiers in GROUP BY statements at execution time.
     *
     * Required so that aliases are tracked properly, regardless of whether
     * group_by() is called prior to from(), join() and dbprefix is added
     * only if needed.
     *
     * @return    string    SQL statement
     */
    protected function _compile_group_by()
    {
        if (count($this->qb_groupby) > 0) {
            for ($i = 0, $c = count($this->qb_groupby); $i < $c; $i++) {
                // Is it already compiled?
                if (is_string($this->qb_groupby[$i])) {
                    continue;
                }

                $this->qb_groupby[$i] = ($this->qb_groupby[$i]['escape'] === FALSE OR $this->_is_literal($this->qb_groupby[$i]['field']))
                    ? $this->qb_groupby[$i]['field']
                    : $this->protect_identifiers($this->qb_groupby[$i]['field']);
            }

            return "\nGROUP BY " . implode(', ', $this->qb_groupby);
        }

        return '';
    }

    // --------------------------------------------------------------------

    /**
     * Compile ORDER BY
     *
     * Escapes identifiers in ORDER BY statements at execution time.
     *
     * Required so that aliases are tracked properly, regardless of whether
     * order_by() is called prior to from(), join() and dbprefix is added
     * only if needed.
     *
     * @return    string    SQL statement
     */
    protected function _compile_order_by()
    {
        if (empty($this->qb_orderby)) {
            return '';
        }

        for ($i = 0, $c = count($this->qb_orderby); $i < $c; $i++) {
            if (is_string($this->qb_orderby[$i])) {
                continue;
            }

            if ($this->qb_orderby[$i]['escape'] !== FALSE && !$this->_is_literal($this->qb_orderby[$i]['field'])) {
                $this->qb_orderby[$i]['field'] = $this->protect_identifiers($this->qb_orderby[$i]['field']);
            }

            $this->qb_orderby[$i] = $this->qb_orderby[$i]['field'] . $this->qb_orderby[$i]['direction'];
        }

        return "\nORDER BY " . implode(', ', $this->qb_orderby);
    }

    // --------------------------------------------------------------------

    /**
     * Object to Array
     *
     * Takes an object as input and converts the class variables to array key/vals
     *
     * @param    object
     * @return    array
     */
    protected function _object_to_array($object)
    {
        if (!is_object($object)) {
            return $object;
        }

        $array = array();
        foreach (get_object_vars($object) as $key => $val) {
            // There are some built in keys we need to ignore for this conversion
            if (!is_object($val) && !is_array($val) && $key !== '_parent_name') {
                $array[$key] = $val;
            }
        }

        return $array;
    }

    // --------------------------------------------------------------------

    /**
     * Object to Array
     *
     * Takes an object as input and converts the class variables to array key/vals
     *
     * @param    object
     * @return    array
     */
    protected function _object_to_array_batch($object)
    {
        if (!is_object($object)) {
            return $object;
        }

        $array = array();
        $out = get_object_vars($object);
        $fields = array_keys($out);

        foreach ($fields as $val) {
            // There are some built in keys we need to ignore for this conversion
            if ($val !== '_parent_name') {
                $i = 0;
                foreach ($out[$val] as $data) {
                    $array[$i++][$val] = $data;
                }
            }
        }

        return $array;
    }

    // --------------------------------------------------------------------

    /**
     * Start Cache
     *
     * Starts QB caching
     *
     * @return    CI_DB_query_builder
     */
    public function start_cache()
    {
        $this->qb_caching = TRUE;
        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Stop Cache
     *
     * Stops QB caching
     *
     * @return    CI_DB_query_builder
     */
    public function stop_cache()
    {
        $this->qb_caching = FALSE;
        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Flush Cache
     *
     * Empties the QB cache
     *
     * @return    CI_DB_query_builder
     */
    public function flush_cache()
    {
        $this->_reset_run(array(
            'qb_cache_select' => array(),
            'qb_cache_from' => array(),
            'qb_cache_join' => array(),
            'qb_cache_where' => array(),
            'qb_cache_groupby' => array(),
            'qb_cache_having' => array(),
            'qb_cache_orderby' => array(),
            'qb_cache_set' => array(),
            'qb_cache_exists' => array(),
            'qb_cache_no_escape' => array(),
            'qb_cache_aliased_tables' => array()
        ));

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Merge Cache
     *
     * When called, this function merges any cached QB arrays with
     * locally called ones.
     *
     * @return    void
     */
    protected function _merge_cache()
    {
        if (count($this->qb_cache_exists) === 0) {
            return;
        } elseif (in_array('select', $this->qb_cache_exists, TRUE)) {
            $qb_no_escape = $this->qb_cache_no_escape;
        }

        foreach (array_unique($this->qb_cache_exists) as $val) // select, from, etc.
        {
            $qb_variable = 'qb_' . $val;
            $qb_cache_var = 'qb_cache_' . $val;
            $qb_new = $this->$qb_cache_var;

            for ($i = 0, $c = count($this->$qb_variable); $i < $c; $i++) {
                if (!in_array($this->{$qb_variable}[$i], $qb_new, TRUE)) {
                    $qb_new[] = $this->{$qb_variable}[$i];
                    if ($val === 'select') {
                        $qb_no_escape[] = $this->qb_no_escape[$i];
                    }
                }
            }

            $this->$qb_variable = $qb_new;
            if ($val === 'select') {
                $this->qb_no_escape = $qb_no_escape;
            }
        }
    }

    // --------------------------------------------------------------------

    /**
     * Is literal
     *
     * Determines if a string represents a literal value or a field name
     *
     * @param    string $str
     * @return    bool
     */
    protected function _is_literal($str)
    {
        $str = trim($str);

        if (empty($str) OR ctype_digit($str) OR (string)(float)$str === $str OR in_array(strtoupper($str), array('TRUE', 'FALSE'), TRUE)) {
            return TRUE;
        }

        static $_str;

        if (empty($_str)) {
            $_str = ($this->_escape_char !== '"')
                ? array('"', "'") : array("'");
        }

        return in_array($str[0], $_str, TRUE);
    }

    // --------------------------------------------------------------------

    /**
     * Reset Query Builder values.
     *
     * Publicly-visible method to reset the QB values.
     *
     * @return    CI_DB_query_builder
     */
    public function reset_query()
    {
        $this->_reset_select();
        $this->_reset_write();
        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Resets the query builder values.  Called by the get() function
     *
     * @param    array    An array of fields to reset
     * @return    void
     */
    protected function _reset_run($qb_reset_items)
    {
        foreach ($qb_reset_items as $item => $default_value) {
            $this->$item = $default_value;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Resets the query builder values.  Called by the get() function
     *
     * @return    void
     */
    protected function _reset_select()
    {
        $this->_reset_run(array(
            'qb_select' => array(),
            'qb_from' => array(),
            'qb_join' => array(),
            'qb_where' => array(),
            'qb_groupby' => array(),
            'qb_having' => array(),
            'qb_orderby' => array(),
            'qb_aliased_tables' => array(),
            'qb_no_escape' => array(),
            'qb_distinct' => FALSE,
            'qb_limit' => FALSE,
            'qb_offset' => FALSE
        ));
    }

    // --------------------------------------------------------------------

    /**
     * Resets the query builder "write" values.
     *
     * Called by the insert() update() insert_batch() update_batch() and delete() functions
     *
     * @return    void
     */
    protected function _reset_write()
    {
        $this->_reset_run(array(
            'qb_set' => array(),
            'qb_set_ub' => array(),
            'qb_from' => array(),
            'qb_join' => array(),
            'qb_where' => array(),
            'qb_orderby' => array(),
            'qb_keys' => array(),
            'qb_limit' => FALSE
        ));
    }

}


/**
 * MySQLi Database Adapter Class
 *
 * Note: _DB is an extender class that the app controller
 * creates dynamically based on whether the query builder
 * class is being used or not.
 *
 * @package        CodeIgniter
 * @subpackage    Drivers
 * @category    Database
 * @author        EllisLab Dev Team
 * @link        https://codeigniter.com/user_guide/database/
 */
class CI_DB_mysqli_driver extends CI_DB_query_builder
{

    /**
     * Database driver
     *
     * @var    string
     */
    public $dbdriver = 'mysqli';

    /**
     * Compression flag
     *
     * @var    bool
     */
    public $compress = FALSE;

    /**
     * DELETE hack flag
     *
     * Whether to use the MySQL "delete hack" which allows the number
     * of affected rows to be shown. Uses a preg_replace when enabled,
     * adding a bit more processing to all queries.
     *
     * @var    bool
     */
    public $delete_hack = TRUE;

    /**
     * Strict ON flag
     *
     * Whether we're running in strict SQL mode.
     *
     * @var    bool
     */
    public $stricton;

    // --------------------------------------------------------------------

    /**
     * Identifier escape character
     *
     * @var    string
     */
    protected $_escape_char = '`';

    // --------------------------------------------------------------------

    /**
     * MySQLi object
     *
     * Has to be preserved without being assigned to $conn_id.
     *
     * @var    MySQLi
     */
    protected $_mysqli;

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @param    bool $persistent
     * @return    object
     */
    public function db_connect($persistent = FALSE)
    {
        // Do we have a socket path?
        if ($this->hostname[0] === '/') {
            $hostname = NULL;
            $port = NULL;
            $socket = $this->hostname;
        } else {
            $hostname = ($persistent === TRUE)
                ? 'p:' . $this->hostname : $this->hostname;
            $port = empty($this->port) ? NULL : $this->port;
            $socket = NULL;
        }

        $client_flags = ($this->compress === TRUE) ? MYSQLI_CLIENT_COMPRESS : 0;
        $this->_mysqli = mysqli_init();

        $this->_mysqli->options(MYSQLI_OPT_CONNECT_TIMEOUT, 10);

        if (isset($this->stricton)) {
            if ($this->stricton) {
                $this->_mysqli->options(MYSQLI_INIT_COMMAND, 'SET SESSION sql_mode = CONCAT(@@sql_mode, ",", "STRICT_ALL_TABLES")');
            } else {
                $this->_mysqli->options(MYSQLI_INIT_COMMAND,
                    'SET SESSION sql_mode =
					REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
					@@sql_mode,
					"STRICT_ALL_TABLES,", ""),
					",STRICT_ALL_TABLES", ""),
					"STRICT_ALL_TABLES", ""),
					"STRICT_TRANS_TABLES,", ""),
					",STRICT_TRANS_TABLES", ""),
					"STRICT_TRANS_TABLES", "")'
                );
            }
        }

        if (is_array($this->encrypt)) {
            $ssl = array();
            empty($this->encrypt['ssl_key']) OR $ssl['key'] = $this->encrypt['ssl_key'];
            empty($this->encrypt['ssl_cert']) OR $ssl['cert'] = $this->encrypt['ssl_cert'];
            empty($this->encrypt['ssl_ca']) OR $ssl['ca'] = $this->encrypt['ssl_ca'];
            empty($this->encrypt['ssl_capath']) OR $ssl['capath'] = $this->encrypt['ssl_capath'];
            empty($this->encrypt['ssl_cipher']) OR $ssl['cipher'] = $this->encrypt['ssl_cipher'];

            if (isset($this->encrypt['ssl_verify'])) {
                $client_flags |= MYSQLI_CLIENT_SSL;

                if ($this->encrypt['ssl_verify']) {
                    defined('MYSQLI_OPT_SSL_VERIFY_SERVER_CERT') && $this->_mysqli->options(MYSQLI_OPT_SSL_VERIFY_SERVER_CERT, TRUE);
                }
                // Apparently (when it exists), setting MYSQLI_OPT_SSL_VERIFY_SERVER_CERT
                // to FALSE didn't do anything, so PHP 5.6.16 introduced yet another
                // constant ...
                //
                // https://secure.php.net/ChangeLog-5.php#5.6.16
                // https://bugs.php.net/bug.php?id=68344
                elseif (defined('MYSQLI_CLIENT_SSL_DONT_VERIFY_SERVER_CERT')) {
                    $client_flags |= MYSQLI_CLIENT_SSL_DONT_VERIFY_SERVER_CERT;
                }
            }

            if (!empty($ssl)) {
                $client_flags |= MYSQLI_CLIENT_SSL;
                $this->_mysqli->ssl_set(
                    isset($ssl['key']) ? $ssl['key'] : NULL,
                    isset($ssl['cert']) ? $ssl['cert'] : NULL,
                    isset($ssl['ca']) ? $ssl['ca'] : NULL,
                    isset($ssl['capath']) ? $ssl['capath'] : NULL,
                    isset($ssl['cipher']) ? $ssl['cipher'] : NULL
                );
            }
        }

        if ($this->_mysqli->real_connect($hostname, $this->username, $this->password, $this->database, $port, $socket, $client_flags)) {
            // Prior to version 5.7.3, MySQL silently downgrades to an unencrypted connection if SSL setup fails
            if (
                ($client_flags & MYSQLI_CLIENT_SSL)
                && version_compare($this->_mysqli->client_info, '5.7.3', '<=')
                && empty($this->_mysqli->query("SHOW STATUS LIKE 'ssl_cipher'")->fetch_object()->Value)
            ) {
                $this->_mysqli->close();
                $message = 'MySQLi was configured for an SSL connection, but got an unencrypted connection instead!';
                log_message('error', $message);
                return ($this->db_debug) ? $this->display_error($message, '', TRUE) : FALSE;
            }

            return $this->_mysqli;
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Reconnect
     *
     * Keep / reestablish the db connection if no queries have been
     * sent for a length of time exceeding the server's idle timeout
     *
     * @return    void
     */
    public function reconnect()
    {
        if ($this->conn_id !== FALSE && $this->conn_id->ping() === FALSE) {
            $this->conn_id = FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Select the database
     *
     * @param    string $database
     * @return    bool
     */
    public function db_select($database = '')
    {
        if ($database === '') {
            $database = $this->database;
        }

        if ($this->conn_id->select_db($database)) {
            $this->database = $database;
            $this->data_cache = array();
            return TRUE;
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Set client character set
     *
     * @param    string $charset
     * @return    bool
     */
    protected function _db_set_charset($charset)
    {
        return $this->conn_id->set_charset($charset);
    }

    // --------------------------------------------------------------------

    /**
     * Database version number
     *
     * @return    string
     */
    public function version()
    {
        if (isset($this->data_cache['version'])) {
            return $this->data_cache['version'];
        }

        return $this->data_cache['version'] = $this->conn_id->server_info;
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @param    string $sql an SQL query
     * @return    mixed
     */
    protected function _execute($sql)
    {
        return $this->conn_id->query($this->_prep_query($sql));
    }

    // --------------------------------------------------------------------

    /**
     * Prep the query
     *
     * If needed, each database adapter can prep the query string
     *
     * @param    string $sql an SQL query
     * @return    string
     */
    protected function _prep_query($sql)
    {
        // mysqli_affected_rows() returns 0 for "DELETE FROM TABLE" queries. This hack
        // modifies the query so that it a proper number of affected rows is returned.
        if ($this->delete_hack === TRUE && preg_match('/^\s*DELETE\s+FROM\s+(\S+)\s*$/i', $sql)) {
            return trim($sql) . ' WHERE 1=1';
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Begin Transaction
     *
     * @return    bool
     */
    protected function _trans_begin()
    {
        $this->conn_id->autocommit(FALSE);
        return is_php('5.5')
            ? $this->conn_id->begin_transaction()
            : $this->simple_query('START TRANSACTION'); // can also be BEGIN or BEGIN WORK
    }

    // --------------------------------------------------------------------

    /**
     * Commit Transaction
     *
     * @return    bool
     */
    protected function _trans_commit()
    {
        if ($this->conn_id->commit()) {
            $this->conn_id->autocommit(TRUE);
            return TRUE;
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Rollback Transaction
     *
     * @return    bool
     */
    protected function _trans_rollback()
    {
        if ($this->conn_id->rollback()) {
            $this->conn_id->autocommit(TRUE);
            return TRUE;
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Platform-dependent string escape
     *
     * @param    string
     * @return    string
     */
    protected function _escape_str($str)
    {
        return $this->conn_id->real_escape_string($str);
    }

    // --------------------------------------------------------------------

    /**
     * Affected Rows
     *
     * @return    int
     */
    public function affected_rows()
    {
        return $this->conn_id->affected_rows;
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @return    int
     */
    public function insert_id()
    {
        return $this->conn_id->insert_id;
    }

    // --------------------------------------------------------------------

    /**
     * List table query
     *
     * Generates a platform-specific query string so that the table names can be fetched
     *
     * @param    bool $prefix_limit
     * @return    string
     */
    protected function _list_tables($prefix_limit = FALSE)
    {
        $sql = 'SHOW TABLES FROM ' . $this->escape_identifiers($this->database);

        if ($prefix_limit !== FALSE && $this->dbprefix !== '') {
            return $sql . " LIKE '" . $this->escape_like_str($this->dbprefix) . "%'";
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Show column query
     *
     * Generates a platform-specific query string so that the column names can be fetched
     *
     * @param    string $table
     * @return    string
     */
    protected function _list_columns($table = '')
    {
        return 'SHOW COLUMNS FROM ' . $this->protect_identifiers($table, TRUE, NULL, FALSE);
    }

    // --------------------------------------------------------------------

    /**
     * Returns an object with field data
     *
     * @param    string $table
     * @return    array
     */
    public function field_data($table)
    {
        if (($query = $this->query('SHOW COLUMNS FROM ' . $this->protect_identifiers($table, TRUE, NULL, FALSE))) === FALSE) {
            return FALSE;
        }
        $query = $query->result_object();

        $retval = array();
        for ($i = 0, $c = count($query); $i < $c; $i++) {
            $retval[$i] = new stdClass();
            $retval[$i]->name = $query[$i]->Field;

            sscanf($query[$i]->Type, '%[a-z](%d)',
                $retval[$i]->type,
                $retval[$i]->max_length
            );

            $retval[$i]->default = $query[$i]->Default;
            $retval[$i]->primary_key = (int)($query[$i]->Key === 'PRI');
        }

        return $retval;
    }

    // --------------------------------------------------------------------

    /**
     * Error
     *
     * Returns an array containing code and message of the last
     * database error that has occurred.
     *
     * @return    array
     */
    public function error()
    {
        if (!empty($this->_mysqli->connect_errno)) {
            return array(
                'code' => $this->_mysqli->connect_errno,
                'message' => $this->_mysqli->connect_error
            );
        }

        return array('code' => $this->conn_id->errno, 'message' => $this->conn_id->error);
    }

    // --------------------------------------------------------------------

    /**
     * FROM tables
     *
     * Groups tables in FROM clauses if needed, so there is no confusion
     * about operator precedence.
     *
     * @return    string
     */
    protected function _from_tables()
    {
        if (!empty($this->qb_join) && count($this->qb_from) > 1) {
            return '(' . implode(', ', $this->qb_from) . ')';
        }

        return implode(', ', $this->qb_from);
    }

    // --------------------------------------------------------------------

    /**
     * Close DB Connection
     *
     * @return    void
     */
    protected function _close()
    {
        $this->conn_id->close();
    }

}


/**
 * Database Result Class
 *
 * This is the platform-independent result class.
 * This class will not be called directly. Rather, the adapter
 * class for the specific database will extend and instantiate it.
 *
 * @category    Database
 * @author        EllisLab Dev Team
 * @link        https://codeigniter.com/user_guide/database/
 */
class CI_DB_result
{

    /**
     * Connection ID
     *
     * @var    resource|object
     */
    public $conn_id;

    /**
     * Result ID
     *
     * @var    resource|object
     */
    public $result_id;

    /**
     * Result Array
     *
     * @var    array[]
     */
    public $result_array = array();

    /**
     * Result Object
     *
     * @var    object[]
     */
    public $result_object = array();

    /**
     * Custom Result Object
     *
     * @var    object[]
     */
    public $custom_result_object = array();

    /**
     * Current Row index
     *
     * @var    int
     */
    public $current_row = 0;

    /**
     * Number of rows
     *
     * @var    int
     */
    public $num_rows;

    /**
     * Row data
     *
     * @var    array
     */
    public $row_data;

    // --------------------------------------------------------------------

    /**
     * Constructor
     *
     * @param    object $driver_object
     * @return    void
     */
    public function __construct(&$driver_object)
    {
        $this->conn_id = $driver_object->conn_id;
        $this->result_id = $driver_object->result_id;
    }

    // --------------------------------------------------------------------

    /**
     * Number of rows in the result set
     *
     * @return    int
     */
    public function num_rows()
    {
        if (is_int($this->num_rows)) {
            return $this->num_rows;
        } elseif (count($this->result_array) > 0) {
            return $this->num_rows = count($this->result_array);
        } elseif (count($this->result_object) > 0) {
            return $this->num_rows = count($this->result_object);
        }

        return $this->num_rows = count($this->result_array());
    }

    // --------------------------------------------------------------------

    /**
     * Query result. Acts as a wrapper function for the following functions.
     *
     * @param    string $type 'object', 'array' or a custom class name
     * @return    array
     */
    public function result($type = 'object')
    {
        if ($type === 'array') {
            return $this->result_array();
        } elseif ($type === 'object') {
            return $this->result_object();
        }

        return $this->custom_result_object($type);
    }

    // --------------------------------------------------------------------

    /**
     * Custom query result.
     *
     * @param    string $class_name
     * @return    array
     */
    public function custom_result_object($class_name)
    {
        if (isset($this->custom_result_object[$class_name])) {
            return $this->custom_result_object[$class_name];
        } elseif (!$this->result_id OR $this->num_rows === 0) {
            return array();
        }

        // Don't fetch the result set again if we already have it
        $_data = NULL;
        if (($c = count($this->result_array)) > 0) {
            $_data = 'result_array';
        } elseif (($c = count($this->result_object)) > 0) {
            $_data = 'result_object';
        }

        if ($_data !== NULL) {
            for ($i = 0; $i < $c; $i++) {
                $this->custom_result_object[$class_name][$i] = new $class_name();

                foreach ($this->{$_data}[$i] as $key => $value) {
                    $this->custom_result_object[$class_name][$i]->$key = $value;
                }
            }

            return $this->custom_result_object[$class_name];
        }

        is_null($this->row_data) OR $this->data_seek(0);
        $this->custom_result_object[$class_name] = array();

        while ($row = $this->_fetch_object($class_name)) {
            $this->custom_result_object[$class_name][] = $row;
        }

        return $this->custom_result_object[$class_name];
    }

    // --------------------------------------------------------------------

    /**
     * Query result. "object" version.
     *
     * @return    array
     */
    public function result_object()
    {
        if (count($this->result_object) > 0) {
            return $this->result_object;
        }

        // In the event that query caching is on, the result_id variable
        // will not be a valid resource so we'll simply return an empty
        // array.
        if (!$this->result_id OR $this->num_rows === 0) {
            return array();
        }

        if (($c = count($this->result_array)) > 0) {
            for ($i = 0; $i < $c; $i++) {
                $this->result_object[$i] = (object)$this->result_array[$i];
            }

            return $this->result_object;
        }

        is_null($this->row_data) OR $this->data_seek(0);
        while ($row = $this->_fetch_object()) {
            $this->result_object[] = $row;
        }

        return $this->result_object;
    }

    // --------------------------------------------------------------------

    /**
     * Query result. "array" version.
     *
     * @return    array
     */
    public function result_array()
    {
        if (count($this->result_array) > 0) {
            return $this->result_array;
        }

        // In the event that query caching is on, the result_id variable
        // will not be a valid resource so we'll simply return an empty
        // array.
        if (!$this->result_id OR $this->num_rows === 0) {
            return array();
        }

        if (($c = count($this->result_object)) > 0) {
            for ($i = 0; $i < $c; $i++) {
                $this->result_array[$i] = (array)$this->result_object[$i];
            }

            return $this->result_array;
        }

        is_null($this->row_data) OR $this->data_seek(0);
        while ($row = $this->_fetch_assoc()) {
            $this->result_array[] = $row;
        }

        return $this->result_array;
    }

    // --------------------------------------------------------------------

    /**
     * Row
     *
     * A wrapper method.
     *
     * @param    mixed $n
     * @param    string $type 'object' or 'array'
     * @return    mixed
     */
    public function row($n = 0, $type = 'object')
    {
        if (!is_numeric($n)) {
            // We cache the row data for subsequent uses
            is_array($this->row_data) OR $this->row_data = $this->row_array(0);

            // array_key_exists() instead of isset() to allow for NULL values
            if (empty($this->row_data) OR !array_key_exists($n, $this->row_data)) {
                return NULL;
            }

            return $this->row_data[$n];
        }

        if ($type === 'object') return $this->row_object($n);
        elseif ($type === 'array') return $this->row_array($n);

        return $this->custom_row_object($n, $type);
    }

    // --------------------------------------------------------------------

    /**
     * Assigns an item into a particular column slot
     *
     * @param    mixed $key
     * @param    mixed $value
     * @return    void
     */
    public function set_row($key, $value = NULL)
    {
        // We cache the row data for subsequent uses
        if (!is_array($this->row_data)) {
            $this->row_data = $this->row_array(0);
        }

        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $this->row_data[$k] = $v;
            }
            return;
        }

        if ($key !== '' && $value !== NULL) {
            $this->row_data[$key] = $value;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Returns a single result row - custom object version
     *
     * @param    int $n
     * @param    string $type
     * @return    object
     */
    public function custom_row_object($n, $type)
    {
        isset($this->custom_result_object[$type]) OR $this->custom_result_object($type);

        if (count($this->custom_result_object[$type]) === 0) {
            return NULL;
        }

        if ($n !== $this->current_row && isset($this->custom_result_object[$type][$n])) {
            $this->current_row = $n;
        }

        return $this->custom_result_object[$type][$this->current_row];
    }

    // --------------------------------------------------------------------

    /**
     * Returns a single result row - object version
     *
     * @param    int $n
     * @return    object
     */
    public function row_object($n = 0)
    {
        $result = $this->result_object();
        if (count($result) === 0) {
            return NULL;
        }

        if ($n !== $this->current_row && isset($result[$n])) {
            $this->current_row = $n;
        }

        return $result[$this->current_row];
    }

    // --------------------------------------------------------------------

    /**
     * Returns a single result row - array version
     *
     * @param    int $n
     * @return    array
     */
    public function row_array($n = 0)
    {
        $result = $this->result_array();
        if (count($result) === 0) {
            return NULL;
        }

        if ($n !== $this->current_row && isset($result[$n])) {
            $this->current_row = $n;
        }

        return $result[$this->current_row];
    }

    // --------------------------------------------------------------------

    /**
     * Returns the "first" row
     *
     * @param    string $type
     * @return    mixed
     */
    public function first_row($type = 'object')
    {
        $result = $this->result($type);
        return (count($result) === 0) ? NULL : $result[0];
    }

    // --------------------------------------------------------------------

    /**
     * Returns the "last" row
     *
     * @param    string $type
     * @return    mixed
     */
    public function last_row($type = 'object')
    {
        $result = $this->result($type);
        return (count($result) === 0) ? NULL : $result[count($result) - 1];
    }

    // --------------------------------------------------------------------

    /**
     * Returns the "next" row
     *
     * @param    string $type
     * @return    mixed
     */
    public function next_row($type = 'object')
    {
        $result = $this->result($type);
        if (count($result) === 0) {
            return NULL;
        }

        return isset($result[$this->current_row + 1])
            ? $result[++$this->current_row]
            : NULL;
    }

    // --------------------------------------------------------------------

    /**
     * Returns the "previous" row
     *
     * @param    string $type
     * @return    mixed
     */
    public function previous_row($type = 'object')
    {
        $result = $this->result($type);
        if (count($result) === 0) {
            return NULL;
        }

        if (isset($result[$this->current_row - 1])) {
            --$this->current_row;
        }
        return $result[$this->current_row];
    }

    // --------------------------------------------------------------------

    /**
     * Returns an unbuffered row and move pointer to next row
     *
     * @param    string $type 'array', 'object' or a custom class name
     * @return    mixed
     */
    public function unbuffered_row($type = 'object')
    {
        if ($type === 'array') {
            return $this->_fetch_assoc();
        } elseif ($type === 'object') {
            return $this->_fetch_object();
        }

        return $this->_fetch_object($type);
    }

    // --------------------------------------------------------------------

    /**
     * The following methods are normally overloaded by the identically named
     * methods in the platform-specific driver -- except when query caching
     * is used. When caching is enabled we do not load the other driver.
     * These functions are primarily here to prevent undefined function errors
     * when a cached result object is in use. They are not otherwise fully
     * operational due to the unavailability of the database resource IDs with
     * cached results.
     */

    // --------------------------------------------------------------------

    /**
     * Number of fields in the result set
     *
     * Overridden by driver result classes.
     *
     * @return    int
     */
    public function num_fields()
    {
        return 0;
    }

    // --------------------------------------------------------------------

    /**
     * Fetch Field Names
     *
     * Generates an array of column names.
     *
     * Overridden by driver result classes.
     *
     * @return    array
     */
    public function list_fields()
    {
        return array();
    }

    // --------------------------------------------------------------------

    /**
     * Field data
     *
     * Generates an array of objects containing field meta-data.
     *
     * Overridden by driver result classes.
     *
     * @return    array
     */
    public function field_data()
    {
        return array();
    }

    // --------------------------------------------------------------------

    /**
     * Free the result
     *
     * Overridden by driver result classes.
     *
     * @return    void
     */
    public function free_result()
    {
        $this->result_id = FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Data Seek
     *
     * Moves the internal pointer to the desired offset. We call
     * this internally before fetching results to make sure the
     * result set starts at zero.
     *
     * Overridden by driver result classes.
     *
     * @param    int $n
     * @return    bool
     */
    public function data_seek($n = 0)
    {
        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Result - associative array
     *
     * Returns the result set as an array.
     *
     * Overridden by driver result classes.
     *
     * @return    array
     */
    protected function _fetch_assoc()
    {
        return array();
    }

    // --------------------------------------------------------------------

    /**
     * Result - object
     *
     * Returns the result set as an object.
     *
     * Overridden by driver result classes.
     *
     * @param    string $class_name
     * @return    object
     */
    protected function _fetch_object($class_name = 'stdClass')
    {
        return new $class_name();
    }

}


/**
 * MySQLi Result Class
 *
 * This class extends the parent result class: CI_DB_result
 *
 * @package        CodeIgniter
 * @subpackage    Drivers
 * @category    Database
 * @author        EllisLab Dev Team
 * @link        https://codeigniter.com/user_guide/database/
 */
class CI_DB_mysqli_result extends CI_DB_result
{

    /**
     * Number of rows in the result set
     *
     * @return    int
     */
    public function num_rows()
    {
        return is_int($this->num_rows)
            ? $this->num_rows
            : $this->num_rows = $this->result_id->num_rows;
    }

    // --------------------------------------------------------------------

    /**
     * Number of fields in the result set
     *
     * @return    int
     */
    public function num_fields()
    {
        return $this->result_id->field_count;
    }

    // --------------------------------------------------------------------

    /**
     * Fetch Field Names
     *
     * Generates an array of column names
     *
     * @return    array
     */
    public function list_fields()
    {
        $field_names = array();
        $this->result_id->field_seek(0);
        while ($field = $this->result_id->fetch_field()) {
            $field_names[] = $field->name;
        }

        return $field_names;
    }

    // --------------------------------------------------------------------

    /**
     * Field data
     *
     * Generates an array of objects containing field meta-data
     *
     * @return    array
     */
    public function field_data()
    {
        $retval = array();
        $field_data = $this->result_id->fetch_fields();
        for ($i = 0, $c = count($field_data); $i < $c; $i++) {
            $retval[$i] = new stdClass();
            $retval[$i]->name = $field_data[$i]->name;
            $retval[$i]->type = static::_get_field_type($field_data[$i]->type);
            $retval[$i]->max_length = $field_data[$i]->max_length;
            $retval[$i]->primary_key = (int)($field_data[$i]->flags & MYSQLI_PRI_KEY_FLAG);
            $retval[$i]->default = $field_data[$i]->def;
        }

        return $retval;
    }

    // --------------------------------------------------------------------

    /**
     * Get field type
     *
     * Extracts field type info from the bitflags returned by
     * mysqli_result::fetch_fields()
     *
     * @used-by    CI_DB_mysqli_result::field_data()
     * @param    int $type
     * @return    string
     */
    private static function _get_field_type($type)
    {
        static $map;
        isset($map) OR $map = array(
            MYSQLI_TYPE_DECIMAL => 'decimal',
            MYSQLI_TYPE_BIT => 'bit',
            MYSQLI_TYPE_TINY => 'tinyint',
            MYSQLI_TYPE_SHORT => 'smallint',
            MYSQLI_TYPE_INT24 => 'mediumint',
            MYSQLI_TYPE_LONG => 'int',
            MYSQLI_TYPE_LONGLONG => 'bigint',
            MYSQLI_TYPE_FLOAT => 'float',
            MYSQLI_TYPE_DOUBLE => 'double',
            MYSQLI_TYPE_TIMESTAMP => 'timestamp',
            MYSQLI_TYPE_DATE => 'date',
            MYSQLI_TYPE_TIME => 'time',
            MYSQLI_TYPE_DATETIME => 'datetime',
            MYSQLI_TYPE_YEAR => 'year',
            MYSQLI_TYPE_NEWDATE => 'date',
            MYSQLI_TYPE_INTERVAL => 'interval',
            MYSQLI_TYPE_ENUM => 'enum',
            MYSQLI_TYPE_SET => 'set',
            MYSQLI_TYPE_TINY_BLOB => 'tinyblob',
            MYSQLI_TYPE_MEDIUM_BLOB => 'mediumblob',
            MYSQLI_TYPE_BLOB => 'blob',
            MYSQLI_TYPE_LONG_BLOB => 'longblob',
            MYSQLI_TYPE_STRING => 'char',
            MYSQLI_TYPE_VAR_STRING => 'varchar',
            MYSQLI_TYPE_GEOMETRY => 'geometry'
        );

        return isset($map[$type]) ? $map[$type] : $type;
    }

    // --------------------------------------------------------------------

    /**
     * Free the result
     *
     * @return    void
     */
    public function free_result()
    {
        if (is_object($this->result_id)) {
            $this->result_id->free();
            $this->result_id = FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Data Seek
     *
     * Moves the internal pointer to the desired offset. We call
     * this internally before fetching results to make sure the
     * result set starts at zero.
     *
     * @param    int $n
     * @return    bool
     */
    public function data_seek($n = 0)
    {
        return $this->result_id->data_seek($n);
    }

    // --------------------------------------------------------------------

    /**
     * Result - associative array
     *
     * Returns the result set as an array
     *
     * @return    array
     */
    protected function _fetch_assoc()
    {
        return $this->result_id->fetch_assoc();
    }

    // --------------------------------------------------------------------

    /**
     * Result - object
     *
     * Returns the result set as an object
     *
     * @param    string $class_name
     * @return    object
     */
    protected function _fetch_object($class_name = 'stdClass')
    {
        return $this->result_id->fetch_object($class_name);
    }

}


/**
 * Database Cache Class
 *
 * @category    Database
 * @author        EllisLab Dev Team
 * @link        https://codeigniter.com/user_guide/database/
 */
class CI_DB_Cache
{

    /**
     * CI Singleton
     *
     * @var    object
     */
    public $CI;

    /**
     * Database object
     *
     * Allows passing of DB object so that multiple database connections
     * and returned DB objects can be supported.
     *
     * @var    object
     */
    public $db;

    // --------------------------------------------------------------------

    /**
     * Constructor
     *
     * @param    object &$db
     * @return    void
     */
    public function __construct(&$db)
    {
        // Assign the main CI object to $this->CI and load the file helper since we use it a lot
        $this->CI =& get_instance();
        $this->db =& $db;
        $this->CI->load->helper('file');

        $this->check_path();
    }

    // --------------------------------------------------------------------

    /**
     * Set Cache Directory Path
     *
     * @param    string $path Path to the cache directory
     * @return    bool
     */
    public function check_path($path = '')
    {
        if ($path === '') {
            if ($this->db->cachedir === '') {
                return $this->db->cache_off();
            }

            $path = $this->db->cachedir;
        }

        // Add a trailing slash to the path if needed
        $path = realpath($path)
            ? rtrim(realpath($path), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR
            : rtrim($path, '/') . '/';

        if (!is_dir($path)) {
            log_message('debug', 'DB cache path error: ' . $path);

            // If the path is wrong we'll turn off caching
            return $this->db->cache_off();
        }

        if (!is_really_writable($path)) {
            log_message('debug', 'DB cache dir not writable: ' . $path);

            // If the path is not really writable we'll turn off caching
            return $this->db->cache_off();
        }

        $this->db->cachedir = $path;
        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Retrieve a cached query
     *
     * The URI being requested will become the name of the cache sub-folder.
     * An MD5 hash of the SQL statement will become the cache file name.
     *
     * @param    string $sql
     * @return    string
     */
    public function read($sql)
    {
        $segment_one = ($this->CI->uri->segment(1) == FALSE) ? 'default' : $this->CI->uri->segment(1);
        $segment_two = ($this->CI->uri->segment(2) == FALSE) ? 'index' : $this->CI->uri->segment(2);
        $filepath = $this->db->cachedir . $segment_one . '+' . $segment_two . '/' . md5($sql);

        if (!is_file($filepath) OR FALSE === ($cachedata = file_get_contents($filepath))) {
            return FALSE;
        }

        return unserialize($cachedata);
    }

    // --------------------------------------------------------------------

    /**
     * Write a query to a cache file
     *
     * @param    string $sql
     * @param    object $object
     * @return    bool
     */
    public function write($sql, $object)
    {
        $segment_one = ($this->CI->uri->segment(1) == FALSE) ? 'default' : $this->CI->uri->segment(1);
        $segment_two = ($this->CI->uri->segment(2) == FALSE) ? 'index' : $this->CI->uri->segment(2);
        $dir_path = $this->db->cachedir . $segment_one . '+' . $segment_two . '/';
        $filename = md5($sql);

        if (!is_dir($dir_path) && !@mkdir($dir_path, 0750)) {
            return FALSE;
        }

        if (write_file($dir_path . $filename, serialize($object)) === FALSE) {
            return FALSE;
        }

        chmod($dir_path . $filename, 0640);
        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Delete cache files within a particular directory
     *
     * @param    string $segment_one
     * @param    string $segment_two
     * @return    void
     */
    public function delete($segment_one = '', $segment_two = '')
    {
        if ($segment_one === '') {
            $segment_one = ($this->CI->uri->segment(1) == FALSE) ? 'default' : $this->CI->uri->segment(1);
        }

        if ($segment_two === '') {
            $segment_two = ($this->CI->uri->segment(2) == FALSE) ? 'index' : $this->CI->uri->segment(2);
        }

        $dir_path = $this->db->cachedir . $segment_one . '+' . $segment_two . '/';
        delete_files($dir_path, TRUE);
    }

    // --------------------------------------------------------------------

    /**
     * Delete all existing cache files
     *
     * @return    void
     */
    public function delete_all()
    {
        delete_files($this->db->cachedir, TRUE, TRUE);
    }

}
