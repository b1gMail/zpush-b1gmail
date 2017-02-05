<?php
/*
 * Project         : b1gMail backend for Z-Push
 * File            : blobstorage.php
 * Description     : Blob storage providers to access message data.
 * Created         : 05.02.2017
 *
 * Copyright (C) 2017 Patrick Schlangen <ps@b1g.de>
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

define('BMBLOB_TYPE_MAIL',					0);

define('BMBLOBSTORAGE_SEPARATEFILES',		0);
define('BMBLOBSTORAGE_USERDB',				1);

/**
 * blob storage provider interface
 */
interface BMBlobStorageInterface
{
	/**
	 * set backend instance
	 *
	 * @param BackendB1GMail $instance
	 */
	public function setBackendInstance($instance);

	/**
	 * store a blob
	 *
	 * @param int $type
	 * @param int $id
	 * @param mixed $data Either string or resource (stream)
	 * @param int $limit Max no. of bytes to copy from stream
	 * @return bool
	 */
	public function storeBlob($type, $id, $data, $limit = -1);

	/**
	 * load a blob
	 *
	 * @param int $type
	 * @param int $id
	 * @return resource stream
	 */
	public function loadBlob($type, $id);

	/**
	 * delete a blob
	 *
	 * @param int $type
	 * @param int $id
	 * @return bool
	 */
	public function deleteBlob($type, $id);

	/**
	 * open provider for a certain user (called by factory)
	 *
	 * @param int $userID
	 */
	public function open($userID);
}

abstract class BMAbstractBlobStorage implements BMBlobStorageInterface
{
	/**
	 * backend instance
	 */
	protected $backend;

	/**
	 * set backend instance
	 *
	 * @param BackendB1GMail $instance
	 */
	public function setBackendInstance($instance)
	{
		$this->backend = $instance;
	}

	/**
	 * user ID
	 */
	protected $userID;

	/**
	 * open provider for a certain user
	 *
	 * @param int $userID
	 */
	public function open($userID)
	{
		$this->userID = $userID;
	}
}

class BMBlobStorage_SeparateFiles extends BMAbstractBlobStorage
{
	private $extMap = array(
		BMBLOB_TYPE_MAIL	=> 'msg'
	);

	public function storeBlob($type, $id, $data, $limit = -1)
	{
		$ext = $this->extMap[$type];
		$fileName = $this->backend->DataFilename($id, $ext);

		$fp = fopen($fileName, 'wb');
		if(!is_resource($fp))
		{
			ZLog::Write(LOGLEVEL_ERROR, sprintf('Failed to open blob file <%s> for writing (type: %d, id: %d)',
				$fileName, $type, $id));
			return(false);
		}
		if(is_resource($data))
		{
			$byteCount = 0;
			while(!feof($data))
			{
				$chunk = fread($data, 4096);
				if($limit != -1 && $byteCount+strlen($chunk) > $limit)
					break;
				fwrite($fp, $chunk);
				$byteCount += strlen($chunk);
			}
		}
		else
		{
			if($limit > -1 && strlen($data) > $limit)
				$data = substr($data, 0, $limit);
			fwrite($fp, $data);
		}
		fclose($fp);

		@chmod($fileName, 0666);

		return(true);
	}

	public function loadBlob($type, $id)
	{
		$ext = $this->extMap[$type];
		$fileName = $this->backend->DataFilename($id, $ext, true);

		if(!file_exists($fileName))
		{
			ZLog::Write(LOGLEVEL_ERROR, sprintf('Blob file <%s> does not exist (type: %d, id: %d)',
					$fileName, $type, $id));
			return(false);
		}

		$fp = fopen($fileName, 'rb');
		if(!is_resource($fp))
		{
			ZLog::Write(LOGLEVEL_ERROR, sprintf('Failed to open blob file <%s> for reading (type: %d, id: %d)',
					$fileName, $type, $id));
		}

		return($fp);
	}

	public function deleteBlob($type, $id)
	{
		$ext = $this->extMap[$type];
		$fileName = $this->backend->DataFilename($id, $ext, true);

		if(file_exists($fileName))
			return(@unlink($fileName));

		return(true);
	}
}

define('BMBS_USERDB_FLAG_GZCOMPRESSED',			1);

class BMBlobStorage_UserDB extends BMAbstractBlobStorage
{
	/**
	 * SQLite db object
	 */
	private $sdb = false;

	/**
	 * Active transactions counter
	 */
	private $txCounter = 0;

	/**
	 * Compression level for compressed blobs
	 */
	private $compressLevel = 8;

	public function open($userID)
	{
		parent::open($userID);

		$dbFileName = $this->getDBFileName();
		if(!file_exists($dbFileName))
			@touch($dbFileName);
		@chmod($dbFileName, 0666);

		$this->sdb = new SQLite3($dbFileName);
		$this->initDB();
	}

	public function __destruct()
	{
		if($this->sdb !== false)
			$this->sdb->close();
	}

	private function getDBFileName()
	{
		return($this->backend->DataFilename($this->userID, 'blobdb'));
	}

	private function initDB()
	{
		$this->sdb->busyTimeout(15000);

		$this->sdb->query('CREATE TABLE IF NOT EXISTS [blobs] ('
			. '	[type] INTEGER,'
			. '	[id] INTEGER,'
			. '	[flags] INTEGER,'
			. ' [size] INTEGER,'
			. '	[data] BLOB,'
			. '	PRIMARY KEY([type],[id])'
			. ')');
	}

	public function storeBlob($type, $id, $data, $limit = -1)
	{
		if(is_resource($data))
		{
			$fp = $data;
			$data = '';
			while(!feof($fp))
				$data .= fread($fp, 4096);
		}

		if($limit > -1 && strlen($data) > $limit)
			$data = substr($data, 0, $limit);

		$dataSize = strlen($data);

		$flags = 0;
		if(($type == BMBLOB_TYPE_MAIL && $this->backend->prefs['blobstorage_compress'] == 'yes')
			&& function_exists('gzcompress'))
		{
			$data = gzcompress($data, $this->compressLevel);
			$flags |= BMBS_USERDB_FLAG_GZCOMPRESSED;
		}

		$stmt = $this->sdb->prepare('REPLACE INTO [blobs]([type],[id],[data],[flags],[size]) VALUES(:type,:id,:data,:flags,:size)');
		$stmt->bindValue(':type', 	$type, 		SQLITE3_INTEGER);
		$stmt->bindValue(':id', 	$id, 		SQLITE3_INTEGER);
		$stmt->bindValue(':data', 	$data, 		SQLITE3_BLOB);
		$stmt->bindValue(':flags', 	$flags, 	SQLITE3_INTEGER);
		$stmt->bindValue(':size',	$dataSize,	SQLITE3_INTEGER);
		$stmt->execute();

		return(true);
	}

	public function loadBlob($type, $id)
	{
		$stmt = $this->sdb->prepare('SELECT [data],[flags] FROM [blobs] WHERE [type]=:type AND [id]=:id');
		$stmt->bindValue(':type',	$type, 	SQLITE3_INTEGER);
		$stmt->bindValue(':id',		$id,	SQLITE3_INTEGER);
		$res = $stmt->execute();

		$result = false;
		if($row = $res->fetchArray(SQLITE3_ASSOC))
		{
			if(($row['flags'] & BMBS_USERDB_FLAG_GZCOMPRESSED) != 0)
				$row['data'] = gzuncompress($row['data']);

			$result = fopen('php://temp', 'wb+');
			fwrite($result, $row['data']);
			fseek($result, 0, SEEK_SET);
		}
		$res->finalize();

		return($result);
	}

	public function deleteBlob($type, $id)
	{
		$stmt = $this->sdb->prepare('DELETE FROM [blobs] WHERE [type]=:type AND [id]=:id');
		$stmt->bindValue(':type', 	$type, 	SQLITE3_INTEGER);
		$stmt->bindValue(':id', 	$id, 	SQLITE3_INTEGER);
		$stmt->execute();

		return(true);
	}
}
