<?php 
/*
 * Project         : b1gMail backend for Z-Push
 * File            : db.php
 * Description     : MySQL client functions wrapper class. Used to interface with
 *                   the MySQL database.
 * Created         : 27.01.2013
 *
 * Copyright (C) 2013 Patrick Schlangen <ps@b1g.de>
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

class DB
{
	private $handle;
	
	/**
	 * constructor
	 *
	 * @param resource $handle MySQL connection
	 */
	public function __construct($handle)
	{
		$this->handle = $handle;
	}
	
	/**
	 * set connection charset
	 *
	 * @param string $charset
	 */
	function SetCharset($charset)
	{
		mysqli_set_charset($this->handle, $charset);
	}

	/**
	 * escape a string for use in SQL query
	 *
	 * @param string $str String
	 * @return string
	 */
	public function Escape($str)
	{
		return(mysqli_real_escape_string($this->handle, $str));
	}
	
	/**
	 * execute safe query
	 *
	 * @param string $query
	 * @return DB_Result
	 */
	public function Query($query)
	{
		// replace {pre} with prefix
		$query = str_replace('{pre}', B1GMAIL_DB_PREFIX, $query);
		
		// insert escaped values, if any
		if(func_num_args() > 1)
		{
			$args = func_get_args();
			$pos = 0;
			
			for($i=1; $i<func_num_args(); $i++)
			{
				$pos = strpos($query, '?', $pos);
				if($pos === false)
				{
					break;
				}
				else 
				{
					if(is_string($args[$i]) && (strcmp($args[$i], '#NULL#') == 0))
					{
						$intxt = 'NULL';
					}
					else if(is_array($args[$i]))
					{
						$intxt = '';
						foreach($args[$i] as $val)
							$intxt .= ',\'' . $this->Escape($val) . '\'';
						$intxt = '(' . substr($intxt, 1) . ')';
						if($intxt == '()')
							$intxt = '(0)';
					}
					else
					{
						$intxt = '\'' . $this->Escape($args[$i]) . '\'';
					}
					
					$query = substr_replace($query, $intxt, $pos, 1);
					$pos += strlen($intxt);
				}
			}
		}

		$ok = ($result = mysqli_query($this->handle, $query));
		
		// return new MySQL_Result object if query was successful
		if($ok)
		{
			return(isset($result) ? new DB_Result($this->handle, $result, $query) : false);
		}
		else 
		{
			throw new FatalException("MySQL-Error at '" . $_SERVER['SCRIPT_NAME'] . "': '" . mysqli_error($this->handle) . "', tried to execute '" . $query . "'", 0, null, LOGLEVEL_FATAL);
			die();
			return(false);
		}
	}
	
	/**
	 * get insert id
	 *
	 * @return int
	 */
	public function InsertId()
	{
		return(mysqli_insert_id($this->handle));
	}
	
	/**
	 * get number of affected rows
	 *
	 * @return int
	 */
	public function AffectedRows()
	{
		return(mysqli_affected_rows($this->handle));
	}
}

class DB_Result
{
	private $handle;
	private $result;
	private $query;
	
	/**
	 * constructor
	 *
	 * @param resource $handle
	 * @param resource $result
	 * @return DB_Result
	 */
	public function __construct($handle, $result, $query = '')
	{
		$this->handle = $handle;
		$this->result = $result;
		$this->query = $query;
	}
	
	/**
	 * fetch a row as array
	 *
	 * @return array
	 */
	public function FetchArray($resultType = MYSQLI_BOTH)
	{
		return(mysqli_fetch_array($this->result, $resultType));
	}
	
	/**
	 * fetch a row as object
	 *
	 * @return object
	 */
	public function FetchObject()
	{
		return(mysqli_fetch_object($this->result));
	}
	
	/**
	 * get count of rows in result set
	 *
	 * @return int
	 */
	public function RowCount()
	{
		return(mysqli_num_rows($this->result));
	}
	
	/**
	 * get field count
	 *
	 * @return int
	 */
	public function FieldCount()
	{
		return(mysqli_num_fields($this->result));
	}
	
	/**
	 * get field name
	 * 
	 * @param int $index Index
	 * @return string
	 */
	public function FieldName($index)
	{
		$field = mysqli_fetch_field_direct($this->result, $index);
		return($field->name);
	}
	
	/**
	 * free result
	 *
	 */
	public function Free()
	{
		@mysqli_free_result($this->result);
	}
}
