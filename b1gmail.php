<?php 
/*
 * Project         : b1gMail backend for Z-Push
 * File            : b1gmail.php
 * Description     : This is the main file of the b1gMail backend. It connects
 *                   Z-Push to a b1gMail database.
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

include_once('lib/default/diffbackend/diffbackend.php');
require_once('backend/b1gmail/db.php');
require_once('backend/b1gmail/sendmail.php');
require_once('include/mimeDecode.php');
require_once('include/z_RFC822.php');
require_once('include/stringstreamwrapper.php');

class BackendB1GMail extends BackendDiff
{
	private $dbHandle;
	private $db;
	private $prefs;
	private $loggedOn = false;
	private $userID = 0;
	private $userRow;
	private $groupRow;
	
	/**
	 * class constructor
	 */
	public function __construct()
	{
		// check if configuration constants exists
		if(!defined('B1GMAIL_DB_HOST') || !defined('B1GMAIL_DB_USER') || !defined('B1GMAIL_DB_PASS')
			|| !defined('B1GMAIL_DB_DB') || !defined('B1GMAIL_DB_PREFIX'))
		{
			throw new FatalException('b1gMail backend not configured', 0, null, LOGLEVEL_FATAL);
		}
		
		// connect to database
		$this->dbHandle = mysql_connect(B1GMAIL_DB_HOST, B1GMAIL_DB_USER, B1GMAIL_DB_PASS);
		if(!$this->dbHandle)
			throw new FatalException('Failed to connect to b1gMail MySQL server', 0, null, LOGLEVEL_FATAL);
		if(!mysql_select_db(B1GMAIL_DB_DB, $this->dbHandle))
			throw new FatalException('Failed to select b1gMail MySQL DB', 0, null, LOGLEVEL_FATAL);
		$this->db = new DB($this->dbHandle);
		$this->db->Query('SET NAMES utf8');
		
		// read preferences
		$this->prefs = $this->GetPrefs();
	}
	
	/**
	 * class destructor
	 */
	public function __destruct()
	{
		// close database connection
		if($this->dbHandle)
			mysql_close($this->dbHandle);
	}
	
	/**
	 * Logon function
	 *
	 * @param string $username Username
	 * @param string $domain Domain name (ignored)
	 * @param string $password Password
	 * @return bool
	 */
	public function Logon($username, $domain, $password)
	{
		// get user row
		$res = $this->db->Query('SELECT * FROM {pre}users WHERE `email`=? LIMIT 1',
			$username);
		if($res->RowCount() != 1)
		{
			ZLog::Write(LOGLEVEL_ERROR, sprintf('Login as "%s" failed: user not found', $username));
			return(false);
		}
		$userRow = $res->FetchArray(MYSQL_ASSOC);
		$res->Free();
		
		// locked?
		if($userRow['gesperrt'] != 'no')
		{
			ZLog::Write(LOGLEVEL_ERROR, sprintf('Login as "%s" failed: user locked', $username));
			return(false);
		}
		
		// check password
		if($userRow['passwort'] != md5(md5($password) . $userRow['passwort_salt']))
		{
			ZLog::Write(LOGLEVEL_ERROR, sprintf('Login as "%s" failed: wrong password', $username));
			return(false);
		}
		
		// check group permission
		$res = $this->db->Query('SELECT * FROM {pre}gruppen WHERE `id`=?',
			$userRow['gruppe']);
		if($res->RowCount() != 1)
		{
			ZLog::Write(LOGLEVEL_ERROR, sprintf('Login as "%s" failed: user group not found', $username));
			return(false);
		}
		$groupRow = $res->FetchArray(MYSQL_ASSOC);
		$res->Free();
		if($groupRow['syncml'] != 'yes')
		{
			ZLog::Write(LOGLEVEL_ERROR, sprintf('Login as "%s" failed: synchronization not allowed in group settings', $username));
			return(false);
		}

		// login successful
		$this->loggedOn 	= true;
		$this->userID 		= $userRow['id'];
		$this->userRow		= $userRow;
		$this->groupRow 	= $groupRow;
		return(true);
	}
	
	/**
	 * Logoff function
	 */
	public function Logoff()
	{
		if($this->loggedOn)
		{
			// log out
			$this->loggedOn 	= false;
			$this->userID		= 0;
			$this->userRow		= false;
		}
		
		$this->SaveStorages();
	}
	
	/**
	 * Send mail, save copy in outbox
	 *
	 * @param SendMail $sm
	 * @return bool
	 */
	public function SendMail($sm)
	{
		ZLog::Write(LOGLEVEL_DEBUG, 'b1gMail::SendMail()');

		// check sending frequency
		if(($this->userRow['last_send'] + $this->groupRow['send_limit']) > time())
		{
			ZLog::Write(LOGLEVEL_INFO, 'SendMail(): Message rejected: Maximum sending frequency violation');
			return(false);
		}

		// parse message
		$mimeParser = new Mail_mimeDecode($sm->mime);
		$parsedMail = $mimeParser->decode(array(
			'decode_headers' => true,
			'decode_bodies' => false,
			'include_bodies' => false,
			'charset' => 'utf-8'));

		// extract recipients
		$recipientsStr = '';
		if(!empty($parsedMail->headers['to']))
			$recipientsStr .= ' ' . $parsedMail->headers['to'];
		if(!empty($parsedMail->headers['cc']))
			$recipientsStr .= ' ' . $parsedMail->headers['cc'];
		if(!empty($parsedMail->headers['bcc']))
			$recipientsStr .= ' ' . $parsedMail->headers['bcc'];
		$recipients = $this->ExtractMailAddresses($recipientsStr);
		$sender = $this->ExtractMailAddress($parsedMail->headers['from']);

		// check recipient limit
		if(count($recipients) > $this->groupRow['max_recps'])
		{
			ZLog::Write(LOGLEVEL_INFO, 'SendMail(): Message rejected: Too many recipients');
			return(false);
		}

		// check if sender matches account address / alias
		if(!in_array(strtolower($sender), $this->GetPossibleSenders()))
		{
			ZLog::Write(LOGLEVEL_INFO, 'SendMail(): Message rejected: Sender not allowed (does not match any account email address)');
			return(false);
		}

		// copy to temp stream
		$fp = fopen('php://temp', 'wb+');
		fwrite($fp, $sm->mime);
		fseek($fp, 0, SEEK_SET);

		// send
		$sendmail = new b1gMailSendMail($this->prefs);
		$sendmail->SetUserID($this->userID);
		$sendmail->SetSender($sender);
		$sendmail->SetMailFrom($this->userRow['email']);
		$sendmail->SetRecipients($recipients);
		$sendmail->SetSubject(isset($parsedMail->headers['subject'])
			? $parsedMail->headers['subject']
			: '(no subject)');
		$sendmail->SetBodyStream($fp);
		$result = $sendmail->Send();

		// close temp stream
		fclose($fp);

		if($result)
		{
			// update last send
			$this->db->Query('UPDATE {pre}users SET `last_send`=?,`sent_mails`=`sent_mails`+? WHERE `id`=?',
				time(),
				count($recipients),
				$this->userID);

			// date?
			$date = @strtotime($parsedMail->headers['date']);
			if($date <= 0)
				$date = time();

			// priority?
			if(isset($parsedMail->headers['x-priority']))
			{
				switch((int)$parsedMail->headers['x-priority'])
				{
				case 1:
					$priority = 'high';
					break;
				
				case 5:
					$priority = 'normal';
					break;
					
				default:
					$priority = 'low';
					break;
				}
			}
			else
			{
				$priority = 'normal';
			}

			// extract message ID
			$messageIDs = $this->ExtractMessageIDs($parsedMail->headers['message-id']);
			$messageID = count($messageIDs) > 0
				? $messageIDs[0]
				: '<' . uniqid() . '@' . $this->prefs['b1gmta_host'] . '>';

			// references
			$references = '';
			if(!empty($parsedMail->headers['references']))
				$references .= ' ' . $parsedMail->headers['references'];
			if(!empty($parsedMail->headers['in-reply-to']))
				$references .= ' ' . $parsedMail->headers['in-reply-to'];
			$references = $this->ExtractMessageIDs($references);

			// flags
			$mailFlags = 0;
			if(!empty($parsedMail->headers['content-type'])
				&& stripos($parsedMail->headers['content-type'], 'multipart/mixed') !== false)		// TODO: implement more sophisticated attachment check
			{
				$mailFlags |= 64; 	// atachment
			}

			// size
			$mailSize = strlen($sm->mime);

			// copy to outbox if enough free space
			if($this->userRow['mailspace_used'] + $mailSize <= $this->groupRow['storage'])
			{
				$this->db->Query('INSERT INTO {pre}mails(userid,betreff,von,an,cc,body,folder,datum,trashstamp,priority,fetched,msg_id,virnam,trained,refs,flags,size) VALUES '
					. '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
					$this->userID,
					!empty($parsedMail->headers['subject']) ? $parsedMail->headers['subject'] 	: '',
					!empty($parsedMail->headers['from']) 	? $parsedMail->headers['from'] 		: '',
					!empty($parsedMail->headers['to']) 		? $parsedMail->headers['to'] 		: '',
					!empty($parsedMail->headers['cc']) 		? $parsedMail->headers['cc'] 		: '',
					'file',
					-2,
					$date,
					0,
					$priority,
					time(),
					$messageID,
					'',
					0,
					implode(';;;', $references),
					$mailFlags,
					$mailSize);
				$mailID = $this->db->InsertId();

				// create file
				if($mailID)
				{
					$fn = $this->DataFilename($mailID);
					if(@file_put_contents($fn, $sm->mime) !== false)
					{
						@chmod($fn, 0666);
					}
					else
					{
						ZLog::Write(LOGLEVEL_ERROR, sprintf('SendMail(): Failed to create mail file %s', $fn));
					}
				}
				else
				{
					ZLog::Write(LOGLEVEL_ERROR, 'SendMail(): Failed to insert mail into database');
				}

				// update space, increment mailbox generation
				$this->UpdateMailSpace($mailSize);
				$this->IncMailboxGeneration();
			}
			else
			{
				ZLog::Write(LOGLEVEL_INFO, 'SendMail(): Message not saved to outbox (not enough free space)');
				return(false);
			}
		}

		return($result);
	}
	
	/**
	 * Get attachment data
	 * 
	 * @param string $attname Attachment name
	 * @return SyncItemOperationsAttachment
	 */
	public function GetAttachmentData($attname)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetAttachmentData(%s)', $attname));
	
		if(strpos($attname, ':') === false)
		{
			ZLog::Write(LOGLEVEL_INFO, 'GetAttachmentData(): Invalid attname');
			return(false);
		}

		// parse attname
		list($mailID, $partID) = explode(':', $attname);

		// get mail row
		$res = $this->db->Query('SELECT `body` FROM {pre}mails WHERE `id`=? AND `userid`=?',
			$mailID,
			$this->userID);
		if($res->RowCount() != 1)
		{
			ZLog::Write(LOGLEVEL_INFO, 'GetAttachmentData(): Mail not found');
			return(false);
		}
		$row = $res->FetchArray(MYSQL_ASSOC);
		$res->Free();
		
		// get message
		if($row['body'] == 'file')
			$mailData = @file_get_contents($this->DataFilename($mailID));
		else
			$mailData = $row['body'];
		unset($row['body']);
			
		// parse message
		$mimeParser = new Mail_mimeDecode($mailData);
		$parsedMail = $mimeParser->decode(array(
			'decode_headers' => true,
			'decode_bodies' => true,
			'include_bodies' => true,
			'charset' => 'utf-8'));

		// get attachments
		$attachments = $this->GetAttachmentsFromParsedMail($parsedMail);

		// free up memory
		unset($parsedMail);
		unset($mimeParser);

		// check part iD
		if(!isset($attachments[$partID]))
		{
			ZLog::Write(LOGLEVEL_INFO, 'GetAttachmentData(): Attachment not found in mail');
			return(false);
		}

		// create result object
		$att = new SyncItemOperationsAttachment();
		$att->data = StringStreamWrapper::Open($attachments[$partID]->body);
		if(isset($attachments[$partID]->ctype_primary) && isset($attachments[$partID]->ctype_secondary))
			$att->contenttype = $attachments[$partID]->ctype_primary . '/' . $attachments[$partID]->ctype_secondary;

		return($att);
	}
	
	/**
	 * Return ID of trash folder
	 *
	 * @return string
	 */
	public function GetWasteBasket()
	{
		ZLog::Write(LOGLEVEL_DEBUG, 'b1gMail::GetWasteBasket()');

		return('.email:-5');
	}

	/**
	 * Get list of available folders
	 *
	 * @return array
	 */
	public function GetFolderList()
	{
		ZLog::Write(LOGLEVEL_DEBUG, 'b1gMail::GetFolderList()');
		
		$result = array();
		
		// system email folders
		$result[] = array(
			'id'		=> '.email:0',
			'parent'	=> '0',
			'mod'		=> 'Inbox'
		);
		$result[] = array(
			'id'		=> '.email:-2',
			'parent'	=> '0',
			'mod'		=> 'Sent'
		);
		$result[] = array(
			'id'		=> '.email:-3',
			'parent'	=> '0',
			'mod'		=> 'Drafts'
		);
		$result[] = array(
			'id'		=> '.email:-4',
			'parent'	=> '0',
			'mod'		=> 'Spam'
		);
		$result[] = array(
			'id'		=> '.email:-5',
			'parent'	=> '0',
			'mod'		=> 'Trash'
		);
		
		// user-created email folders
		$res = $this->db->Query('SELECT `id`,`titel`,`parent` FROM {pre}folders WHERE `userid`=? AND `intelligent`=0 AND `subscribed`=1',
			$this->userID);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			if($row['parent'] <= 0)
				$parentID = '0';
			else
				$parentID = '.email:' . $row['parent'];
			$result[] = array(
				'id'		=> '.email:' . $row['id'],
				'parent'	=> $parentID,
				'mod'		=> $row['titel']
			);
		}
		$res->Free();
		
		// dates
		$result[] = array(
			'id'		=> '.dates:0',
			'parent'	=> '0',
			'mod'		=> 'Calendar'
		);
		// user-created calendars
		$res = $this->db->Query('SELECT `id`,`title` FROM {pre}dates_groups WHERE `user`=?',
			$this->userID);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			$result[] = array(
				'id'		=> '.dates:' . $row['id'],
				'parent'	=> '.dates:0',
				'mod'		=> $row['title']
			);
		}
		$res->Free();
		
		// contacts
		$result[] = array(
			'id'		=> '.contacts',
			'parent'	=> '0',
			'mod'		=> 'Contacts'
		);
		
		// tasks - main task list
		$result[] = array(
			'id'		=> '.tasks:0',
			'parent'	=> '0',
			'mod'		=> 'Tasks'
		);
		// user-created task lists
		$res = $this->db->Query('SELECT `tasklistid`,`title` FROM {pre}tasklists WHERE `userid`=?',
			$this->userID);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			$result[] = array(
				'id'		=> '.tasks:' . $row['tasklistid'],
				'parent'	=> '.tasks:0',
				'mod'		=> $row['title']
			);
		}
		$res->Free();
		
		return($result);
	}
	
	/**
	 * Get folder details.
	 *
	 * @param string $id Folder ID
	 * @return SyncFolder
	 */
	public function GetFolder($id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetFolder(%s)', $id));
		
		// contacts folder
		if($id == '.contacts')
		{
			$folder = new SyncFolder();
			$folder->serverid 		= $id;
			$folder->parentid 		= '0';
			$folder->displayname 	= 'Contacts';
			$folder->type 			= SYNC_FOLDER_TYPE_CONTACT;
			return($folder);
		}
		
		// dates folder
		else if(strlen($id) > 7 && substr($id, 0, 7) == '.dates:')
		{
			list(, $calendarID) = explode(':', $id);
		
			if($calendarID == 0)
			{
				$folder = new SyncFolder();
				$folder->serverid 		= $id;
				$folder->parentid 		= '0';
				$folder->displayname 	= 'Calendar';
				$folder->type 			= SYNC_FOLDER_TYPE_APPOINTMENT;
				return($folder);
			}
			else
			{
				$res = $this->db->Query('SELECT `title` FROM {pre}dates_groups WHERE `user`=? AND `id`=?',
					$this->userID,
					$calendarID);
				if($res->RowCount() != 1) 
					return(false);
				$row = $res->FetchArray(MYSQL_ASSOC);
				$res->Free();
				
				$folder = new SyncFolder();
				$folder->serverid 		= $id;
				$folder->parentid 		= '.dates:0';
				$folder->displayname 	= $row['title'];
				$folder->type 			= SYNC_FOLDER_TYPE_USER_APPOINTMENT;
				return($folder);
			}
		}
		
		// tasks folder
		else if(strlen($id) > 7 && substr($id, 0, 7) == '.tasks:')
		{
			list(, $taskListID) = explode(':', $id);
		
			if($taskListID == 0)
			{
				$folder = new SyncFolder();
				$folder->serverid 		= $id;
				$folder->parentid 		= '0';
				$folder->displayname 	= 'Tasks';
				$folder->type 			= SYNC_FOLDER_TYPE_TASK;
				return($folder);
			}
			else
			{
				$res = $this->db->Query('SELECT `title` FROM {pre}tasklists WHERE `userid`=? AND `tasklistid`=?',
					$this->userID,
					$taskListID);
				if($res->RowCount() != 1) 
					return(false);
				$row = $res->FetchArray(MYSQL_ASSOC);
				$res->Free();
				
				$folder = new SyncFolder();
				$folder->serverid 		= $id;
				$folder->parentid 		= '.tasks:0';
				$folder->displayname 	= $row['title'];
				$folder->type 			= SYNC_FOLDER_TYPE_USER_TASK;
				return($folder);
			}
		}
		
		// email folder
		else if(strlen($id) > 7 && substr($id, 0, 7) == '.email:')
		{
			list(, $folderID) = explode(':', $id);
			
			$folder = new SyncFolder();
			$folder->serverid		= $id;
			$folder->parentid		= '0';
				
			if($folderID == 0)
			{
				$folder->displayname	= 'Inbox';
				$folder->type			= SYNC_FOLDER_TYPE_INBOX;
			}
			else if($folderID == -2)
			{
				$folder->displayname	= 'Sent';
				$folder->type			= SYNC_FOLDER_TYPE_SENTMAIL;
			}
			else if($folderID == -3)
			{
				$folder->displayname	= 'Drafts';
				$folder->type			= SYNC_FOLDER_TYPE_DRAFTS;
			}
			else if($folderID == -4)
			{
				$folder->displayname	= 'Spam';
				$folder->type			= SYNC_FOLDER_TYPE_USER_MAIL;
			}
			else if($folderID == -5)
			{
				$folder->displayname	= 'Trash';
				$folder->type			= SYNC_FOLDER_TYPE_WASTEBASKET;
			}
			else if($folderID > 0)
			{
				$res = $this->db->Query('SELECT `titel`,`parent` FROM {pre}folders WHERE `id`=? AND `userid`=? AND `subscribed`=1',
					$folderID,
					$this->userID);
				while($row = $res->FetchArray(MYSQL_ASSOC))
				{
					if($row['parent'] <= 0)
						$parentID = '0';
					else
						$parentID = '.email:' . $row['parent'];
					
					$folder->parentid		= $parentID;
					$folder->displayname	= $row['titel'];
					$folder->type			= SYNC_FOLDER_TYPE_USER_MAIL;
				}
				$res->Free();
			}
			else
			{
				return(false);
			}
			
			return($folder);
		}
		
		return(false);
	}
	
	/**
	 * Get folder stats.
	 * 
	 * @param string $id Folder ID
	 * @return array
	 */
	public function StatFolder($id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::StatFolder(%s)', $id));
		
		$folder = $this->GetFolder($id);
		if(!$folder)
			return(false);
		
		$result = array(
			'id'		=> $id,
			'parent'	=> $folder->parentid,
			'mod'		=> $folder->displayname
		);
		return($result);
	}
	
	/**
	 * Change a folder.
	 *
	 * @param string $folderid Parent folder ID
	 * @param string $oldid Folder ID
	 * @param int $type Folder type
	 * @return array Same as StatFolder()
	 */
	public function ChangeFolder($folderid, $oldid, $displayname, $type)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::ChangeFolder(%s,%s,%s,%s)',
			$folderid,
			$oldid,
			$displayname,
			$type));
			
		if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.tasks:')
			return($this->ChangeTaskList($folderid, $oldid, $displayname, $type));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.email:')
			return($this->ChangeMailFolder($folderid, $oldid, $displayname, $type));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.dates:')
			return($this->ChangeDateGroup($folderid, $oldid, $displayname, $type));

		return(false);
	}
	
	/**
	 * Internally used function to create/rename a task list.
	 *
	 * @param string $folderid Parent task list ID
	 * @param string $oldid Task list ID
	 * @param int $type Folder type
	 * @return array Same as StatFolder()
	 */
	private function ChangeTaskList($folderid, $oldid, $displayname, $type)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::ChangeTaskList(%s,%s,%s,%s)',
			$folderid,
			$oldid,
			$displayname,
			$type));
		
		// create new task list
		if(empty($oldid))
		{
			$this->db->Query('INSERT INTO {pre}tasklists(`userid`,`title`) VALUES(?,?)',
				$this->userID,
				$displayname);
			$oldid = '.tasks:' . $this->db->InsertId();
		}
		
		// rename existing task list
		else
		{
			list(, $taskListID) = explode(':', $oldid);
			$this->db->Query('UPDATE {pre}tasklists SET `title`=? WHERE `userid`=? AND `tasklistid`=?',
				$displayname,
				$this->userID,
				$taskListID);
		}
		
		return($this->StatFolder($oldid));
	}
	
	/**
	 * Internally used function to create/rename a date group.
	 *
	 * @param string $folderid Parent date group ID
	 * @param string $oldid Date group ID
	 * @param int $type Folder type
	 * @return array Same as StatFolder()
	 */
	private function ChangeDateGroup($folderid, $oldid, $displayname, $type)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::ChangeDateGroup(%s,%s,%s,%s)',
			$folderid,
			$oldid,
			$displayname,
			$type));

		// create new date group
		if(empty($oldid))
		{
			$this->db->Query('INSERT INTO {pre}dates_groups(`user`,`title`) VALUES(?,?)',
				$this->userID,
				$displayname);
			$oldid = '.dates:' . $this->db->InsertId();
		}

		// rename existing date group
		else
		{
			list(, $groupID) = explode(':', $oldid);
			$this->db->Query('UPDATE {pre}dates_groups SET `title`=? WHERE `user`=? AND `id`=?',
				$displayname,
				$this->userID,
				$groupID);
		}

		return($this->StatFolder($oldid));
	}
	
	/**
	 * Internally used function to create/rename an email folder.
	 *
	 * @param string $folderid Parent folder ID
	 * @param string $oldid Folder ID
	 * @param int $type Folder type
	 * @return array Same as StatFolder()
	 */
	private function ChangeMailFolder($folderid, $oldid, $displayname, $type)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::ChangeMailFolder(%s,%s,%s,%s)',
			$folderid,
			$oldid,
			$displayname,
			$type));
		
		list(, $parentFolderID) = explode(':', $folderid);
		if($parentFolderID <= 0)
			$parentFolderID = -1;

		// create new folder
		if(empty($oldid))
		{
			$this->db->Query('INSERT INTO {pre}folders(`userid`,`titel`,`parent`,`subscribed`,`intelligent`) VALUES(?,?,?,?,?)',
				$this->userID,
				$displayname,
				$parentFolderID,
				1,
				0);
			$oldid = '.email:' . $this->db->InsertId();
		}

		// rename existing folder
		else
		{
			list(, $groupID) = explode(':', $oldid);
			$this->db->Query('UPDATE {pre}folders SET `titel`=?,`parent`=? WHERE `userid`=? AND `id`=?',
				$displayname,
				$parentFolderID,
				$this->userID,
				$groupID);
		}
		
		$this->IncMailboxStructureGeneration();
		$this->IncMailboxGeneration();
		
		return($this->StatFolder($oldid));
	}
	
	/**
	 * Delete a folder.
	 *
	 * @param string $id Folder ID
	 * @param string $parentid Parent folder ID
	 * @return bool
	 */
	public function DeleteFolder($id, $parentid)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::DeleteFolder(%s,%s)',
			$id,
			$parentid));
		
		if(strlen($id) > 7 && substr($id, 0, 7) == '.tasks:')
			return($this->DeleteTaskList($id));
		else if(strlen($id) > 7 && substr($id, 0, 7) == '.email:')
			return($this->DeleteMailFolder($id));
		
		return(false);
	}
	
	/**
	 * Delete a task list.
	 *
	 * @param string $id Task list ID
	 * @return bool
	 */
	private function DeleteTaskList($id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::DeleteTaskList(%s)',
			$id));
		
		list(, $taskListID) = explode(':', $id);
		
		$this->db->Query('DELETE FROM {pre}tasks WHERE `tasklistid`=? AND `user`=?',
			$taskListID,
			$this->userID);
		$this->db->Query('DELETE FROM {pre}tasklists WHERE `tasklistid`=? AND `userid`=?',
			$taskListID,
			$this->userID);
		
		return($this->db->AffectedRows() > 0);
	}
	
	/**
	 * Delete a mail folder.
	 *
	 * @param string $id Mail folder ID
	 * @retur bool
	 */
	private function DeleteMailFolder($id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::DeleteMailFolder(%s)',
			$id));
		
		list(, $folderID) = explode(':', $id);
		$result = false;
		
		// delete child folders
		$res = $this->db->Query('SELECT `id` FROM {pre}folders WHERE `parent`=? AND `userid`=?',
			$folderID,
			$this->userID);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			if($row['id'] == $folderID) continue;
			$this->DeleteMailFolder('.email:' . $row['id']);
		}
		$res->Free();
		
		// delete folder
		$this->db->Query('DELETE FROM {pre}folders WHERE `id`=? AND `userid`=?',
			$folderID,
			$this->userID);
		$result = $this->db->AffectedRows() == 1;
		
		if($result)
		{	
			// this folder might have associated folder conditions we need to remove now
			$this->db->Query('DELETE FROM {pre}folder_conditions WHERE `folder`=?',
				$folderID);
			
			// move mails to trash
			$this->db->Query('UPDATE {pre}mails SET `folder`=?,`trashstamp`=? WHERE `folder`=? AND `userid`=?',
				-5,
				time(),
				$folderID,
				$this->userID);
			
			$this->IncMailboxStructureGeneration();
			$this->IncMailboxGeneration();
		}
		
		return($result);
	}
	
	/**
	 * Get message list.
	 *
	 * @param string $folderid Folder ID
	 * @param int $cutoffdate Cut-off date timestamp
	 * @return array
	 */
	public function GetMessageList($folderid, $cutoffdate)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetMessageList(%s,%s)',
			$folderid,
			$cutoffdate));
		
		if($folderid == '.contacts')
			return($this->GetContactsList($folderid, $cutoffdate));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.tasks:')
			return($this->GetTasksList($folderid, $cutoffdate));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.email:')
			return($this->GetMailsList($folderid, $cutoffdate));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.dates:')
			return($this->GetDatesList($folderid, $cutoffdate));
		
		return(false);
	}
	
	/**
	 * Internally used function to retrieve a list of dates.
	 *
	 * @param string $folderid Date group ID
	 * @param int $cutoffdate Cut-off date timestamp
	 * @return array
	 */
	private function GetDatesList($folderid, $cutoffdate)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetDatesList(%s,%s)',
			$folderid,
			$cutoffdate));
		
		list(, $dateGroupID) = explode(':', $folderid);
		if($dateGroupID <= 0)
			$dateGroupID = -1;
		$result = array();

		$res = $this->db->Query('SELECT `id`,`created`,`updated` FROM {pre}dates'
								. ' LEFT JOIN {pre}changelog ON {pre}changelog.`itemtype`=1 AND {pre}changelog.`itemid`={pre}dates.`id`'
								. ' WHERE `user`=? AND `group`=?', $this->userID, $dateGroupID);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			$result[] = array(
				'id'		=> $row['id'],
				'mod'		=> max($row['created'], $row['updated']),
				'flags'		=> 1
			);
		}
		$res->Free();
		
		return($result);
	}
	
	/**
	 * Internally used function to retrieve a list of mails.
	 *
	 * @param string $folderid Folder ID
	 * @param int $cutoffdate Cut-off date timestamp
	 * @return array
	 */
	private function GetMailsList($folderid, $cutoffdate)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetMailsList(%s,%s)',
			$folderid,
			$cutoffdate));

		list(, $mailFolderID) = explode(':', $folderid);
		$result = array();
		
		$res = $this->db->Query('SELECT `id`,`fetched`,`flags` FROM {pre}mails WHERE `userid`=? AND `fetched`>=? AND `folder`=?',
			$this->userID,
			$cutoffdate,
			$mailFolderID);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			$result[] = array(
				'id'		=> $row['id'],
				'mod'		=> $row['fetched'],
				'flags'		=> ($row['flags'] & 1) != 0 ? 0 : 1
			);
		}
		$res->Free();
		
		return($result);
	}
	
	/**
	 * Internally used function to retrieve a list of tasks.
	 *
	 * @param string $folderid Folder ID
	 * @param int $cutoffdate Cut-off date timestamp
	 * @return array
	 */
	private function GetTasksList($folderid, $cutoffdate)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetTasksList(%s,%s)',
			$folderid,
			$cutoffdate));
	
		list(, $taskListID) = explode(':', $folderid);
		$result = array();
		
		$res = $this->db->Query('SELECT `id`,`created`,`updated` FROM {pre}tasks'
								. ' LEFT JOIN {pre}changelog ON {pre}changelog.`itemtype`=2 AND {pre}changelog.`itemid`={pre}tasks.`id`'
								. ' WHERE `user`=? AND `tasklistid`=?', $this->userID, $taskListID);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			$result[] = array(
				'id'		=> $row['id'],
				'mod'		=> max($row['created'], $row['updated']),
				'flags'		=> 1
			);
		}
		$res->Free();

		return($result);
	}
	
	/**
	 * Internally used function to retrieve a list of contacts.
	 *
	 * @param string $folderid Folder ID
	 * @param int $cutoffdate Cut-off date timestamp
	 * @return array
	 */
	private function GetContactsList($folderid, $cutoffdate)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetContactsList(%s,%s)',
			$folderid,
			$cutoffdate));
		
		$result = array();
		
		$res = $this->db->Query('SELECT `id`,`created`,`updated` FROM {pre}adressen'
								. ' LEFT JOIN {pre}changelog ON {pre}changelog.`itemtype`=0 AND {pre}changelog.`itemid`={pre}adressen.`id`'
								. ' WHERE `user`=?', $this->userID);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			$result[] = array(
				'id'		=> $row['id'],
				'mod'		=> max($row['created'], $row['updated']),
				'flags'		=> 1
			);
		}
		$res->Free();
				
		return($result);
	}
	
	/**
	 * Get message details/contents.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Message ID
	 * @param ContentParameters $contentparameters Message content parameters
	 * @return object
	 */
	public function GetMessage($folderid, $id, $contentparameters)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetMessage(%s,%s)',
			$folderid,
			$id));
		
		if($folderid == '.contacts')
			return($this->GetContact($folderid, $id, $contentparameters));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.tasks:')
			return($this->GetTask($folderid, $id, $contentparameters));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.email:')
			return($this->GetMail($folderid, $id, $contentparameters));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.dates:')
			return($this->GetDate($folderid, $id, $contentparameters));
		
		return(false);
	}
	
	/**
	 * Internally used function to get details of a date.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Date ID
	 * @param ContentParameters $contentparameters Message content parameters
	 * @return SyncAppointment
	 */
	private function GetDate($folderid, $id, $contentparameters)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetDate(%s,%s)',
			$folderid,
			$id));
		
		// get date row
		$res = $this->db->Query('SELECT * FROM {pre}dates WHERE `user`=? AND `id`=?',
			$this->userID, $id);
		if($res->RowCount() != 1)
			return(false);
		$row = $res->FetchArray(MYSQL_ASSOC);
		$res->Free();
		
		// create result object
		$result = new SyncAppointment();
		$result->subject		= $row['title'];
		$result->location		= $row['location'];
		$result->alldayevent	= ($row['flags'] & 1) != 0;
		$result->starttime		= $row['startdate'];
		$result->endtime		= $row['enddate'];
		if(($row['flags'] & (2|4)) != 0)
			$result->reminder	= (int)($row['reminder']/60);
		
		// correct start time in case of an all-day event
		if($result->alldayevent)
		{
			$result->starttime	= mktime(0, 0, 0,
				date('m', $result->starttime), date('d', $result->starttime), date('Y', $result->starttime));
		}
		
		// text
		$this->SetBody($result, $row['text']);
		
		// repeating?
		if(($row['repeat_flags'] & (8|16|32|64)) != 0)
		{
			$rec = new SyncRecurrence();
			
			// daily
			if(($row['repeat_flags'] & 8) != 0)
			{
				$rec->type 			= 0;
				$rec->interval		= max(1, $row['repeat_value']);
				$rec->dayofweek		= 1|2|4|8|16|32|64;
				
				// exceptions
				if(strlen($row['repeat_extra1']) > 0)
				{
					$exceptions = explode(',', $row['repeat_extra1']);
					foreach($exceptions as $ex)
						$rec->dayofweek &= ~(1<<$ex);
				}
			}
			
			// weekly
			else if(($row['repeat_flags'] & 16) != 0)
			{
				$rec->type			= 1;
				$rec->interval		= max(1, $row['repeat_value']);
			}
			
			// monthly mday
			else if(($row['repeat_flags'] & 32) != 0)
			{
				$rec->type			= 3;
				$rec->interval		= max(1, $row['repeat_value']);
				$rec->dayofmonth	= max(1, min(31, $row['repeat_extra1']));
			}
			
			// monthly wday
			else if(($row['repeat_flags'] & 64) != 0)
			{
				$rec->type			= 2;
				$rec->weekofmonth	= $row['repeat_extra1'] + 1;
				$rec->dayofweek		= (1<<$row['repeat_extra2']);
			}
			
			// repeat until
			if(($row['repeat_flags'] & 4) != 0)			// date
				$rec->until 		= $row['repeat_times'];
			else if(($row['repeat_flags'] & 2) != 0)	// count
				$rec->occurrences	= $row['repeat_times'] + 1;
			
			$result->recurrence = $rec;
		}
		
		// get attendees
		$result->attendees = array();
		$res = $this->db->Query('SELECT `vorname`,`nachname`,`email`,`work_email`,`default_address` FROM {pre}adressen '
								. 'INNER JOIN {pre}dates_attendees ON {pre}adressen.`id`={pre}dates_attendees.`address` '
								. 'WHERE `user`=? AND `date`=?',
								$this->userID, $id);
		while($attRow = $res->FetchArray(MYSQL_ASSOC))
		{
			$att = new SyncAttendee();
			$att->name 	= trim($attRow['vorname'] . ' ' . $attRow['nachname']);
			if(empty($attRow['email']) || ($attRow['default_address'] == 2 && !empty($attRow['work_email'])))
				$att->email = $attRow['work_email'];
			else
				$att->email = $attRow['email'];
			$result->attendees[] = $att;
		}
		$res->Free();
		
		// unsupported fields
		$result->sensitivity	= 0;
		$result->busystatus		= 2;
		$result->meetingstatus	= 0;
		$result->timezone 		= false;	// TODO: Timezone data generation should be implemented.
		
		return($result);
	}
	
	/**
	 * Internally used function to get details of an email.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Email ID
	 * @param ContentParameters $contentparameters Message content parameters
	 * @return SyncMail
	 */
	private function GetMail($folderid, $id, $contentparameters)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetMail(%s,%s)',
			$folderid,
			$id));
		
		$prioTrans = array(
			'low'		=> 0,
			'normal'	=> 1,
			'high'		=> 2
		);
		
		// get content parameters
		$truncSize 		= Utils::GetTruncSize($contentparameters->GetTruncation());
		$mimeSupport 	= $contentparameters->GetMimeSupport();
		$bodyPreference	= $contentparameters->GetBodyPreference();
		$bodyPrefType	= SYNC_BODYPREFERENCE_PLAIN;
		if($bodyPreference !== false)
		{
			// we cannot handle RTF
			if(in_array(SYNC_BODYPREFERENCE_RTF, $bodyPreference))
				unset($bodyPreference[array_search(SYNC_BODYPREFERENCE_RTF, $bodyPreference)]);
			
			// still entries left in $bodyPreference array => get best match
			if(count($bodyPreference) > 0)
				$bodyPrefType = Utils::GetBodyPreferenceBestMatch($bodyPreference);
		}
		
		// get mail row
		$res = $this->db->Query('SELECT `von`,`betreff`,`flags`,`priority`,`datum`,`body` FROM {pre}mails WHERE `id`=? AND `userid`=?',
			$id,
			$this->userID);
		if($res->RowCount() != 1)
			return(false);
		$row = $res->FetchArray(MYSQL_ASSOC);
		$res->Free();
		
		// create result object
		$result = new SyncMail();
		$result->messageclass	= 'IPM.Note';
		$result->internetcpid	= INTERNET_CPID_UTF8;
		if(Request::GetProtocolVersion() >= 12.0)
			$result->contentclass 	= 'urn:content-classes:message';
		$result->datereceived	= $row['datum'];
		$result->read			= ($row['flags'] & 1) == 0;
		$result->from			= $row['von'];
		$result->subject 		= $row['betreff'];
		$result->importance		= $prioTrans[$row['priority']];
		
		// get message
		if($row['body'] == 'file')
			$mailData = @file_get_contents($this->DataFilename($id));
		else
			$mailData = $row['body'];
		unset($row['body']);
		
		// parse message
		$mimeParser = new Mail_mimeDecode($mailData);
		$parsedMail = $mimeParser->decode(array(
			'decode_headers' => true,
			'decode_bodies' => true,
			'include_bodies' => true,
			'charset' => 'utf-8'));
		
		// parse addresses
		$result->to = $result->cc = $result->reply_to = array();
		$addresses = array('to' => array(), 'cc' => array(), 'reply-to' => array());
		if(!empty($parsedMail->headers['to']))
			$result->to = $this->ExplodeOutsideOfQuotation($parsedMail->headers['to'], array(',', ';'));
		if(!empty($parsedMail->headers['cc']))
			$result->cc = $this->ExplodeOutsideOfQuotation($parsedMail->headers['cc'], array(',', ';'));
		if(!empty($parsedMail->headers['reply-to']))
			$result->reply_to = $this->ExplodeOutsideOfQuotation($parsedMail->headers['reply-to'], array(',', ';'));
		
		// assign other headers which are not available in $row
		if(isset($parsedMail->headers['thread-topic']))
			$result->threadtopic = $parsedMail->headers['thread-topic'];
		
		// get body...
		if(Request::GetProtocolVersion() >= 12.0)
		{
			$result->asbody = new SyncBaseBody();
			
			// get body according to body preference
			if($bodyPrefType == SYNC_BODYPREFERENCE_PLAIN)
			{
				$result->asbody->data = $this->GetTextFromParsedMail($parsedMail, 'plain');
				$result->asbody->type = SYNC_BODYPREFERENCE_PLAIN;
				$result->nativebodytype = SYNC_BODYPREFERENCE_PLAIN;
				
				if(empty($result->asbody->data))
				{
					$result->asbody->data = Utils::ConvertHtmlToText($this->GetTextFromParsedMail($parsedMail, 'html'));
					if(!empty($result->asbody->data))
						$result->nativebodytype = SYNC_BODYPREFERENCE_HTML;
				}
			}
			else if($bodyPrefType == SYNC_BODYPREFERENCE_HTML)
			{
				$result->asbody->data = $this->GetTextFromParsedMail($parsedMail, 'html');
				$result->asbody->type = SYNC_BODYPREFERENCE_HTML;
				$result->nativebodytype = SYNC_BODYPREFERENCE_HTML;
				
				if(empty($result->asbody->data))
				{
					$result->asbody->data = $this->GetTextFromParsedMail($parsedMail, 'plain');
					
					$result->asbody->type = SYNC_BODYPREFERENCE_PLAIN;
					$result->nativebodytype = SYNC_BODYPREFERENCE_PLAIN;
				}
			}
			else if($bodyPrefType == SYNC_BODYPREFERENCE_MIME)
			{
				$result->asbody->data = $mailData;
				$result->asbody->type = SYNC_BODYPREFERENCE_MIME;
				$result->nativebodytype = SYNC_BODYPREFERENCE_MIME;
			}
			else
			{
				ZLog::Write(LOGLEVEL_ERROR, sprintf('Unknown body type: %d', $bodyPrefType));
			}
			
			// truncate, if required
			if(strlen($result->asbody->data) > $truncSize)
			{
				$result->asbody->data = Utils::Utf8_truncate($result->asbody->data, $truncSize);
				$result->asbody->truncated = true;
			}
			else
				$result->asbody->truncated = false;
			
			$result->asbody->estimatedDataSize = strlen($result->asbody->data);
		}
		else
		{
			if($bodyPrefType == SYNC_BODYPREFERENCE_MIME)
			{
				$result->mimedata = $mailData;
				
				if(strlen($result->mimedata) > $truncSize)
				{
					$result->mimedata = substr($result->mimedata, 0, $truncSize);
					$result->mimetruncated = true;
				}
				else
					$result->mimetruncated = false;
				
				$result->mimesize = strlen($result->mimedata);
			}
			else
			{
				$result->body = $this->GetTextFromParsedMail($parsedMail, 'plain');
				if(empty($result->body))
					$result->body = Utils::ConvertHtmlToText($this->GetTextFromParsedMail($parsedMail, 'html'));
				
				if(strlen($result->body) > $truncSize)
				{
					$result->body = substr($result->body, 0, $truncSize);
					$result->bodytruncated = true;
				}
				else
					$result->bodytruncated = false;
				
				$result->bodysize = strlen($result->body);
			}
		}
		
		// ...and attachments
		if($bodyPrefType != SYNC_BODYPREFERENCE_MIME)
		{
			$attachments = $this->GetAttachmentsFromParsedMail($parsedMail);
			
			foreach($attachments as $partID=>$part)
			{
				// get attachment size
				if(isset($part->body))
					$attSize = strlen($part->body);
				else
					$attSize = 0;
				
				// get attachment name
				if(isset($part->d_parameters['filename']))
					$attName = $part->d_parameters['filename'];
				else if(isset($part->d_parameters['name']))
					$attName = $part->d_parameters['name'];
				else
					$attName = 'Unnamed';
				
				// reference string
				$attRef = $id . ':' . $partID;
				
				// content id
				if(isset($part->d_parameters['content-id']))
					$attContentID = $part->d_parameters['content-id'];
				else
					$attContentID = '';
				$attContentID = trim(str_replace(array('<', '>'), '', $attContentID));
				
				if(Request::GetProtocolVersion() >= 12.0)
				{
					$att = new SyncBaseAttachment();
					$att->estimatedDataSize = $attSize;
					$att->displayname 		= $attName;
					$att->filereference 	= $attRef;
					$att->isinline			= isset($part->disposition) && $part->disposition == 'inline';
					$att->contentid			= $attContentID;
					$att->method			= 1;
					
					if(empty($result->asattachments))
						$result->asattachments = array($att);
					else
						$result->asattachments[] = $att;
				}
				else
				{
					$att = new SyncAttachment();
					$att->attsize			= $attSize;
					$att->displayname		= $attName;
					$att->attname			= $attRef;
					$att->attoid			= $attContentID;
					$att->attmethod			= 1;
					
					if(empty($result->attachments))
						$result->attachments = array($att);
					else
						$result->attachments[] = $att;
				}
			}
		}

		return($result);
	}
	
	/**
	 * Internally used function to get details of a task.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Task ID
	 * @param ContentParameters $contentparameters Message content parameters
	 * @return SyncTask
	 */
	private function GetTask($folderid, $id, $contentparameters)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetTask(%s,%s)',
			$folderid,
			$id));
	
		$prioTrans = array(
			'low'		=> 0,
			'normal'	=> 1,
			'high'		=> 2
		);
	
		$res = $this->db->Query('SELECT * FROM {pre}tasks WHERE `user`=? AND `id`=?',
			$this->userID, $id);
		if($res->RowCount() != 1)
			return(false);
		$row = $res->FetchArray(MYSQL_ASSOC);
		$res->Free();
		
		$result = new SyncTask();
		
		if(!empty($row['titel']))			$result->subject			= $row['titel'];
		
		$this->SetBody($result, $row['comments']);
		
		$result->complete		= $row['akt_status'] == 64;
		if($result->complete)	$result->datecompleted = time();
		
		$result->startdate		= $row['beginn'];
		$result->duedate		= $row['faellig'];
		$result->importance		= $prioTrans[$row['priority']];
				
		return($result);
	}
	
	/**
	 * Internally used function to get details of a contact.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Contact ID
	 * @param ContentParameters $contentparameters Message content parameters
	 * @return SyncContact
	 */
	private function GetContact($folderid, $id, $contentparameters)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::GetContact(%s,%s)',
			$folderid,
			$id));
	
		$res = $this->db->Query('SELECT * FROM {pre}adressen WHERE `user`=? AND `id`=?',
			$this->userID, $id);
		if($res->RowCount() != 1)
			return(false);
		$row = $res->FetchArray(MYSQL_ASSOC);
		$res->Free();
		
		$result = new SyncContact();
		
		if(!empty($row['vorname']))			$result->firstname				= $row['vorname'];
		if(!empty($row['nachname']))		$result->lastname				= $row['nachname'];
		
		if(!empty($row['tel']))				$result->homephonenumber		= $row['tel'];
		if(!empty($row['fax']))				$result->homefaxnumber			= $row['fax'];
		if(!empty($row['strassenr']))		$result->homestreet				= $row['strassenr'];
		if(!empty($row['ort']))				$result->homecity				= $row['ort'];
		if(!empty($row['plz']))				$result->homepostalcode			= $row['plz'];
		if(!empty($row['land']))			$result->homecountry			= $row['land'];
		
		if(!empty($row['work_strassenr']))	$result->businessstreet			= $row['work_strassenr'];
		if(!empty($row['work_plz']))		$result->businesspostalcode		= $row['work_plz'];
		if(!empty($row['work_ort']))		$result->businesscity			= $row['work_ort'];
		if(!empty($row['work_land']))		$result->businesscountry		= $row['work_land'];
		if(!empty($row['work_email']))		$result->email2address			= $row['work_email'];
		if(!empty($row['work_tel']))		$result->businessphonenumber	= $row['work_tel'];
		if(!empty($row['work_fax']))		$result->businessfaxnumber		= $row['work_fax'];
		if(!empty($row['work_handy']))		$result->business2phonenumber	= $row['work_handy'];
		
		if(!empty($row['email']))			$result->email1address			= $row['email'];
		if(!empty($row['web']))				$result->webpage				= $row['web'];
		if(!empty($row['handy']))			$result->mobilephonenumber		= $row['handy'];
		if(!empty($row['firma']))			$result->companyname			= $row['firma'];
		if(!empty($row['picture']))
		{
			$picArray = @unserialize($row['picture']);
			if(is_array($picArray) && strlen($picArray['data'])*1.34 <= 49152)	// do not attach too big images to avoid Z-Push dropping the entire contact
				$result->picture		= base64_encode($picArray['data']);
		}
		
		if(!empty($row['position']))		$result->jobtitle				= $row['position'];
		if(!empty($row['geburtsdatum']))	$result->birthday				= $row['geburtsdatum'];
		
		$this->SetBody($result, $row['kommentar']);
		
		return($result);
	}
	
	/**
	 * Get message stats.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Message ID
	 * @return array
	 */
	public function StatMessage($folderid, $id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::StatMessage(%s,%s)',
			$folderid,
			$id));
		
		if($folderid == '.contacts')
			return($this->StatContact($folderid, $id));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.tasks:')
			return($this->StatTask($folderid, $id));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.email:')
			return($this->StatMail($folderid, $id));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.dates:')
			return($this->StatDate($folderid, $id));
		
		return(false);
	}
	
	/**
	 * Internally used function to get date stats.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Date ID
	 * @return array
	 */
	private function StatDate($folderid, $id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::StatDate(%s,%s)',
			$folderid,
			$id));

		$result = false;

		$res = $this->db->Query('SELECT `id`,`created`,`updated` FROM {pre}dates'
								. ' LEFT JOIN {pre}changelog ON {pre}changelog.`itemtype`=1 AND {pre}changelog.`itemid`={pre}dates.`id`'
								. ' WHERE `user`=? AND `id`=?', $this->userID, $id);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			$result = array(
				'id'		=> $row['id'],
				'mod'		=> max($row['created'], $row['updated']),
				'flags'		=> 1
			);
		}
		$res->Free();

		return($result);
	}
	
	/**
	 * Internally used function to get email stats.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Email ID
	 * @return array
	 */
	private function StatMail($folderid, $id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::StatMail(%s,%s)',
			$folderid,
			$id));

		$result = false;

		$res = $this->db->Query('SELECT `id`,`fetched`,`flags` FROM {pre}mails WHERE `userid`=? AND `id`=?',
			$this->userID,
			$id);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			$result = array(
				'id'		=> $row['id'],
				'mod'		=> $row['fetched'],
				'flags'		=> ($row['flags'] & 1) != 0 ? 0 : 1
			);
		}
		$res->Free();

		return($result);
	}
	
	/**
	 * Internally used function to get task stats.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Task ID
	 * @return array
	 */
	private function StatTask($folderid, $id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::StatTask(%s,%s)',
			$folderid,
			$id));

		$result = false;

		$res = $this->db->Query('SELECT `id`,`created`,`updated` FROM {pre}tasks'
								. ' LEFT JOIN {pre}changelog ON {pre}changelog.`itemtype`=2 AND {pre}changelog.`itemid`={pre}tasks.`id`'
								. ' WHERE `user`=? AND `id`=?', $this->userID, $id);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			$result = array(
				'id'		=> $row['id'],
				'mod'		=> max($row['created'], $row['updated']),
				'flags'		=> 1
			);
		}
		$res->Free();

		return($result);
	}
	
	/**
	 * Internally used function to get contact stats.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Contact ID
	 * @return array
	 */
	private function StatContact($folderid, $id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::StatContact(%s,%s)',
			$folderid,
			$id));

		$result = false;

		$res = $this->db->Query('SELECT `id`,`created`,`updated` FROM {pre}adressen'
								. ' LEFT JOIN {pre}changelog ON {pre}changelog.`itemtype`=0 AND {pre}changelog.`itemid`={pre}adressen.`id`'
								. ' WHERE `user`=? AND `id`=?', $this->userID, $id);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			$result = array(
				'id'		=> $row['id'],
				'mod'		=> max($row['created'], $row['updated']),
				'flags'		=> 1
			);
		}
		$res->Free();

		return($result);
	}
	
	/**
	 * Change/create a message.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Message ID
	 * @param object $message New message data
	 * @return array Same as StatMessage($folderid, $id)
	 */
	public function ChangeMessage($folderid, $id, $message, $contentParameters)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::ChangeMessage(%s,%s)',
			$folderid,
			$id));

		// TODO: figure out the role of $contentParameters and implement functionality, if needed

		if($folderid == '.contacts')
			return($this->ChangeContact($folderid, $id, $message));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.tasks:')
			return($this->ChangeTask($folderid, $id, $message));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.dates:')
			return($this->ChangeDate($folderid, $id, $message));

		return(false);
	}
	
	/**
	 * Internally used function to change/create a date.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Date ID
	 * @param SyncAppointment $contact New date data
	 * @return array Same as StatMessage($folderid, $id)
	 */
	private function ChangeDate($folderid, $id, $date)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::ChangeDate(%s,%s)',
			$folderid,
			$id));
		
		// read existing row
		if(!empty($id))
		{
			$res = $this->db->Query('SELECT * FROM {pre}dates WHERE `id`=? AND `user`=?',
				$id, $this->userID);
			if($res->RowCount() != 1)
				return(false);
			$row = $res->FetchArray(MYSQL_ASSOC);
			$res->Free();
		}
		else
			$row = array('flags' => 0);
		
		// basic data
		$row['title']			= $date->subject;
		$row['text']			= $this->GetBody($date);
		$row['location']		= $date->location;
		$row['startdate']		= $date->starttime;
		$row['enddate']			= $date->endtime;
		$row['reminder']		= $date->reminder * 60;
		if($date->alldayevent)
		{
			$row['flags']		|= 1;
			$row['enddate']--;
		}
		else
			$row['flags']		&= ~1;
		if($date->reminder > 0 && ($row['flags'] & (2|4)) == 0)
		{
			$row['flags']		|= 2;	// remind by email - do not assume user wants an SMS reminder when he created the
										// appointment on his internet-capable smartphone
		}
		
		// initialize recurrence data
		$row['repeat_flags']	= 0;
		$row['repeat_times']	= 0;
		$row['repeat_value']	= 0;
		$row['repeat_extra1']	= '';
		$row['repeat_extra2']	= '';
		
		// recurring?
		if(isset($date->recurrence) && is_object($date->recurrence))
		{
			$rec = $date->recurrence;
			
			// daily
			if($rec->type == 0 || ($rec->type == 1 && $rec->interval == 1 && isset($rec->dayofweek)))
			{
				$row['repeat_flags']	|= 8;
				$row['repeat_value']	= max(1, $rec->interval);
				
				// convert dayofweek bitmask to b1gMail exception array
				if(!empty($rec->dayofweek))
				{
					$exceptions = array();
					for($i=0; $i<7; $i++)
					{
						if(($rec->dayofweek & (1<<$i)) == 0)	// day not set in dayofweek -> exception
							$exceptions[] = $i;
					}
				}
				
				$row['repeat_extra1']	= implode(',', $exceptions);
			}
			
			// weekly
			else if($rec->type == 1)
			{
				$row['repeat_flags']	|= 16;
				$row['repeat_value']	= max(1, $rec->interval);
			}
			
			// monthly mday
			else if($rec->type == 3)
			{
				$row['repeat_flags']	|= 32;
				$row['repeat_value']	= max(1, $rec->interval);
				$row['repeat_extra1']	= max(1, min(31, $rec->dayofmonth));
			}
			
			// monthly wday
			else if($rec->type == 2)
			{
				$row['repeat_flags']	|= 64;
				$row['repeat_extra1']	= max(0, min(4, $rec->weekofmonth - 1));
				$row['repeat_extra2']	= max(0, min(6, (int)log($rec->dayofweek, 2)));
			}
			
			// repeat until date...
			if(isset($rec->until) && $rec->until > 0)
			{
				$row['repeat_flags']	|= 4;
				$row['repeat_times']	= $rec->until;
			}
			
			// ...or n times
			else if(isset($rec->occurences) && $rec->occurences > 0)
			{
				$row['repeat_flags']	|= 2;
				$row['repeat_times']	= $rec->occurences + 1;
			}
		}
		
		// create new item
		if(empty($id))
		{
			list(, $groupID) = explode(':', $folderid);
			if($groupID <= 0)
				$groupID = -1;
			
			$this->db->Query('INSERT INTO {pre}dates(`user`,`title`,`location`,`text`,`group`,`startdate`,`enddate`,`reminder`,`flags`, '
							. '`repeat_flags`,`repeat_times`,`repeat_value`,`repeat_extra1`,`repeat_extra2`) '
							. 'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
				$this->userID,
				$row['title'],
				$row['location'],
				$row['text'],
				$groupID,
				$row['startdate'],
				$row['enddate'],
				$row['reminder'],
				$row['flags'],
				$row['repeat_flags'],
				$row['repeat_times'],
				$row['repeat_value'],
				$row['repeat_extra1'],
				$row['repeat_extra2']);
			$id = $this->db->InsertId();
			
			$this->ChangelogAdded(1, $id, time());
		}
		
		// change existing item
		else
		{
			$this->db->Query('UPDATE {pre}dates SET `title`=?,`location`=?,`text`=?,`startdate`=?,`enddate`=?,`reminder`=?,`flags`=?,'
							. '`repeat_flags`=?,`repeat_times`=?,`repeat_value`=?,`repeat_extra1`=?,`repeat_extra2`=? '
							. 'WHERE `id`=? AND `user`=?',
				$row['title'],
				$row['location'],
				$row['text'],
				$row['startdate'],
				$row['enddate'],
				$row['reminder'],
				$row['flags'],
				$row['repeat_flags'],
				$row['repeat_times'],
				$row['repeat_value'],
				$row['repeat_extra1'],
				$row['repeat_extra2'],
				$id,
				$this->userID);
			
			$this->ChangelogUpdated(1, $id, time());
		}
		
		// determine attendee IDs
		$attIDs = array();
		if(isset($date->attendees) && is_array($date->attendees))
		{
			foreach($date->attendees as $att)
			{
				if(!is_object($att))
					continue;
				
				$res = $this->db->Query('SELECT `id` FROM {pre}adressen WHERE (`email`=? OR `work_email`=?) AND `user`=? LIMIT 1',
					$att->email, $att->email, $this->userID);
				if($res->RowCount() != 1)
					continue;
				$attRow = $res->FetchArray(MYSQL_ASSOC);
				$res->Free();
				
				$attIDs[] = $attRow['id'];
			}
		}
		
		// update attendees
		$this->db->Query('DELETE FROM {pre}dates_attendees WHERE `date`=?', $id);
		foreach($attIDs as $attID)
			$this->db->Query('INSERT INTO {pre}dates_attendees(`date`,`address`) VALUES(?,?)', $id, $attID);
		
		return($this->StatMessage($folderid, $id));
	}
	
	/**
	 * Internally used function to change/create a contact.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Contact ID
	 * @param SyncContact $contact New contact data
	 * @return array Same as StatMessage($folderid, $id)
	 */
	private function ChangeContact($folderid, $id, $contact)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::ChangeContact(%s,%s)',
			$folderid,
			$id));
		
		// prepare pic data
		$picData = '';
		if(!empty($contact->picture))
		{
			$picRaw = base64_decode($contact->picture);
			$picData = serialize(array(
				'data'		=> $picRaw,
				'mimeType'	=> $this->GuessPictureType(substr($picRaw, 0, 4))
			));
		}
		
		// create new item
		if(empty($id))
		{
			$this->db->Query('INSERT INTO {pre}adressen(`user`,`vorname`,`nachname`,`tel`,`fax`,`strassenr`,`ort`,`plz`,`land`,'
							. '`work_strassenr`,`work_plz`,`work_ort`,`work_land`,`work_email`,`work_tel`,`work_fax`,`work_handy`,'
							. '`email`,`web`,`handy`,`firma`,`position`,`geburtsdatum`,`picture`,`kommentar`'
							. ') VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
				$this->userID,
				$contact->firstname,
				$contact->lastname,
				$contact->homephonenumber,
				$contact->homefaxnumber,
				$contact->homestreet,
				$contact->homecity,
				$contact->homepostalcode,
				$contact->homecountry,
				$contact->businessstreet,
				$contact->businesspostalcode,
				$contact->businesscity,
				$contact->businesscountry,
				$contact->email2address,
				$contact->businessphonenumber,
				$contact->businessfaxnumber,
				$contact->business2phonenumber,
				$contact->email1address,
				$contact->webpage,
				$contact->mobilephonenumber,
				$contact->companyname,
				$contact->jobtitle,
				$contact->birthday,
				$picData,
				$this->GetBody($contact));
			$id = $this->db->InsertId();
			
			$this->ChangelogAdded(0, $id, time());
		}
		
		// update existing item
		else
		{
			$this->db->Query('UPDATE {pre}adressen SET `vorname`=?,`nachname`=?,`tel`=?,`fax`=?,`strassenr`=?,`ort`=?,`plz`=?,`land`=?,'
				. '`work_strassenr`=?,`work_plz`=?,`work_ort`=?,`work_land`=?,`work_email`=?,`work_tel`=?,`work_fax`=?,`work_handy`=?,'
				. '`email`=?,`web`=?,`handy`=?,`firma`=?,`position`=?,`geburtsdatum`=?,`picture`=?,`kommentar`=? '
				. 'WHERE `id`=? AND `user`=?',
				$contact->firstname,
				$contact->lastname,
				$contact->homephonenumber,
				$contact->homefaxnumber,
				$contact->homestreet,
				$contact->homecity,
				$contact->homepostalcode,
				$contact->homecountry,
				$contact->businessstreet,
				$contact->businesspostalcode,
				$contact->businesscity,
				$contact->businesscountry,
				$contact->email2address,
				$contact->businessphonenumber,
				$contact->businessfaxnumber,
				$contact->business2phonenumber,
				$contact->email1address,
				$contact->webpage,
				$contact->mobilephonenumber,
				$contact->companyname,
				$contact->jobtitle,
				$contact->birthday,
				$picData,
				$this->GetBody($contact),
				$id,
				$this->userID);
			
			$this->ChangelogUpdated(0, $id, time());
		}
		
		return($this->StatMessage($folderid, $id));
	}
	
	/**
	 * Internally used function to change/create a task.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Task ID
	 * @param SyncTask $task New task data
	 * @return array Same as StatMessage($folderid, $id)
	 */
	private function ChangeTask($folderid, $id, $task)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::ChangeTask(%s,%s)',
			$folderid,
			$id));
		
		list(, $taskListID) = explode(':', $folderid);
		$prioTrans = array(
			0	=> 'low',
			1	=> 'normal',
			2	=> 'high'
		);
		
		// create new item
		if(empty($id))
		{
			$this->db->Query('INSERT INTO {pre}tasks(`user`,`tasklistid`,`akt_status`,`beginn`,`faellig`,`priority`,`titel`,`comments`)'
							. ' VALUES(?,?,?,?,?,?,?,?)',
				$this->userID,
				$taskListID,
				$task->complete ? 64 : 16,
				$task->startdate > 0 ? $task->startdate : time(),
				$task->duedate > 0 ? $task->duedate : time()+86400,
				$prioTrans[$task->importance],
				$task->subject,
				$this->GetBody($task));
			$id = $this->db->InsertId();
			
			$this->ChangelogAdded(2, $id, time());
		}
		
		// update existing item
		else
		{
			$res = $this->db->Query('SELECT * FROM {pre}tasks WHERE `id`=? AND `user`=?',
				$id, $this->userID);
			if($res->RowCount() != 1)
				return(false);
			$row = $res->FetchArray(MYSQL_ASSOC);
			$res->Free();
		
			if($task->complete)
				$row['akt_status'] = 64;
			else if($row['akt_status'] == 64)
				$row['akt_status'] = 16;
		
			if($task->startdate > 0)	$row['beginn'] 		= $task->startdate;
			if($task->duedate > 0)		$row['faellig'] 	= $task->duedate;
		
			$row['priority'] 	= $prioTrans[$task->importance];
			$row['titel']		= $task->subject;
			$row['comments']	= $this->GetBody($task);
		
			$this->db->Query('UPDATE {pre}tasks SET `akt_status`=?,`beginn`=?,`faellig`=?,`priority`=?,`titel`=?,`comments`=? WHERE `id`=? AND `user`=?',
				$row['akt_status'],
				$row['beginn'],
				$row['faellig'],
				$row['priority'],
				$row['titel'],
				$row['comments'],
				$id,
				$this->userID);
		
			$this->ChangelogUpdated(2, $id, time());
		}

		return($this->StatMessage($folderid, $id));
	}
	
	/**
	 * Set message read flag.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Mesage ID
	 * @param int $flags Flag
	 * @return bool
	 */
	public function SetReadFlag($folderid, $id, $flags, $contentParameters)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::SetReadFlag(%s,%s,%s)',
			$folderid,
			$id,
			$flags));

		// TODO: figure out the role of $contentParameters and implement functionality, if needed
		
		if(strlen($folderid) <= 7 || substr($folderid, 0, 7) != '.email:')
			return(false);
		
		// get current mail flags
		$res = $this->db->Query('SELECT `flags` FROM {pre}mails WHERE `id`=? AND `userid`=?',
			$id,
			$this->userID);
		if($res->RowCount() != 1)
			return(false);
		list($mailFlags) = $res->FetchArray(MYSQL_NUM);
		$res->Free();
		
		// mark as unread (add b1gMail's unread flag)
		if($flags == 0)
			$mailFlags |= 1;
		
		// mark as read (remove b1gMail's unread flag)
		else if($flags == 1)
			$mailFlags &= ~1;
		
		// set new flags
		$this->db->Query('UPDATE {pre}mails SET `flags`=? WHERE `id`=? AND `userid`=?',
			$mailFlags,
			$id,
			$this->userID);
		
		$this->IncMailboxGeneration();
		
		return(true);
	}
	
	/**
	 * Delete a message.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Message ID
	 * @return bool
	 */
	public function DeleteMessage($folderid, $id, $contentParameters)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::DeleteMessage(%s,%s)',
			$folderid,
			$id));

		// TODO: figure out the role of $contentParameters and implement functionality, if needed

		if($folderid == '.contacts')
			return($this->DeleteContact($folderid, $id));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.tasks:')
			return($this->DeleteTask($folderid, $id));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.dates:')
			return($this->DeleteDate($folderid, $id));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.email:')
			return($this->DeleteMail($folderid, $id));

		return(false);
	}
	
	/**
	 * Internally used function to delete an email.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Email ID
	 * @return bool
	 */
	private function DeleteMail($folderid, $id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::DeleteMail(%s,%s)',
			$folderid,
			$id));
		
		$result = false;
		
		$res = $this->db->Query('SELECT `body`,`size` FROM {pre}mails WHERE `id`=? AND `userid`=?',
			$id,
			$this->userID);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			if($row['body'] == 'file')
			{
				$msgFilename = $this->DataFilename($id);
				@unlink($msgFilename);
			}
			
			$this->db->Query('DELETE FROM {pre}certmails WHERE `mail`=? AND `user`=?',
				$id,
				$this->userID);
			$this->db->Query('DELETE FROM {pre}attachments WHERE `mailid`=?',
				$id);
			$this->db->Query('DELETE FROM {pre}mails WHERE `id`=?',
				$id);
			$this->db->Query('UPDATE {pre}users SET `mailspace_used`=GREATEST(0,`mailspace_used`-?) WHERE `id`=?',
				$row['size'],
				$this->userID);
			
			$this->IncMailboxGeneration();
			
			$result = true;
		}
		$res->Free();
		
		return($result);
	}
	
	/**
	 * Internally used function to delete a date.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Date ID
	 * @return bool
	 */
	private function DeleteDate($folderid, $id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::DeleteDate(%s,%s)',
			$folderid,
			$id));
		
		$this->db->Query('DELETE FROM {pre}dates WHERE `id`=? AND `user`=?',
			$id,
			$this->userID);
		if($this->db->AffectedRows() == 1)
		{
			$this->db->Query('DELETE FROM {pre}dates_attendees WHERE `date`=?',
				$id);
			$this->ChangelogDeleted(1, $id, time());
		}
		return(true);
	}
	
	/**
	 * Internally used function to delete a contact.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Contact ID
	 * @return bool
	 */
	private function DeleteContact($folderid, $id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::DeleteContact(%s,%s)',
			$folderid,
			$id));
		
		$this->db->Query('DELETE FROM {pre}adressen WHERE `id`=? AND `user`=?',
			$id,
			$this->userID);
		if($this->db->AffectedRows() == 1)
		{
			$this->db->Query('DELETE FROM {pre}adressen_gruppen_member WHERE `adresse`=?',
				$id);
			$this->ChangelogDeleted(0, $id, time());
		}
		return(true);
	}
	
	/**
	 * Internally used function to delete a task.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Task ID
	 * @return bool
	 */
	private function DeleteTask($folderid, $id)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::DeleteTask(%s,%s)',
			$folderid,
			$id));
		
		$this->db->Query('DELETE FROM {pre}tasks WHERE `id`=? AND `user`=?',
			$id,
			$this->userID);
		if($this->db->AffectedRows() == 1)
		{
			$this->ChangelogDeleted(2, $id, time());
		}
		return(true);
	}
	
	/**
	 * Move a message to another folder.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Message ID
	 * @param string $newfolderid New folder ID
	 * @return bool
	 */
	public function MoveMessage($folderid, $id, $newfolderid, $contentParameters)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::MoveMessage(%s,%s,%s)',
			$folderid,
			$id,
			$newfolderid));
		
		// TODO: figure out the role of $contentParameters and implement functionality, if needed

		if($folderid == '.contacts')
			return(false);
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.tasks:')
			return($this->MoveTask($folderid, $id, $newfolderid));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.email:')
			return($this->MoveMail($folderid, $id, $newfolderid));
		else if(strlen($folderid) > 7 && substr($folderid, 0, 7) == '.dates:')
			return($this->MoveDate($folderid, $id, $newfolderid));

		return(false);
	}
	
	/**
	 * Internally used function to move a date to another folder.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Date ID
	 * @param string $newfolderid New folder ID
	 * @return bool
	 */
	private function MoveDate($folderid, $id, $newfolderid)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::MoveDate(%s,%s,%s)',
			$folderid,
			$id,
			$newfolderid));
		
		if(strlen($newfolderid) <= 7 || substr($newfolderid, 0, 7) != '.dates:')
			return(false);

		list(, $newGroupID) = explode(':', $newfolderid);
		if($newGroupID == 0)
			$newGroupID = -1;

		$this->db->Query('UPDATE {pre}dates SET `group`=? WHERE `id`=? AND `user`=?',
			$newGroupID,
			$id,
			$this->userID);
		$this->ChangelogUpdated(1, $id, time());
		
		return((string)$id);
	}
	
	/**
	 * Internally used function to move an email to another folder.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Email ID
	 * @param string $newfolderid New folder ID
	 * @return bool
	 */
	private function MoveMail($folderid, $id, $newfolderid)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::MoveMail(%s,%s,%s)',
			$folderid,
			$id,
			$newfolderid));
		
		if(strlen($newfolderid) <= 7 || substr($newfolderid, 0, 7) != '.email:')
			return(false);
		
		list(, $newFolderID) = explode(':', $newfolderid);
		
		$this->db->Query('UPDATE {pre}mails SET `folder`=? WHERE `id`=? AND `userid`=?',
			$newFolderID,
			$id,
			$this->userID);
		
		// update trash timestamp if message has been moved to trash
		if($newFolderID == -5)
		{
			$this->db->Query('UPDATE {pre}mails SET `trashstamp`=? WHERE `id`=? AND `userid`=?',
				time(),
				$id,
				$this->userID);
		}
		
		$this->IncMailboxGeneration();
		
		return((string)$id);
	}
	
	/**
	 * Internally used function to move a task to another folder.
	 *
	 * @param string $folderid Folder ID
	 * @param string $id Task ID
	 * @param string $newfolderid New folder ID
	 * @return bool
	 */
	private function MoveTask($folderid, $id, $newfolderid)
	{
		ZLog::Write(LOGLEVEL_DEBUG, sprintf('b1gMail::MoveTask(%s,%s,%s)',
			$folderid,
			$id,
			$newfolderid));
		
		if(strlen($newfolderid) <= 7 || substr($newfolderid, 0, 7) != '.tasks:')
			return(false);
		
		list(, $newTaskListID) = explode(':', $newfolderid);
		
		$this->db->Query('UPDATE {pre}tasks SET `tasklistid`=? WHERE `id`=? AND `user`=?',
			$newTaskListID,
			$id,
			$this->userID);
		$this->ChangelogUpdated(2, $id, time());
		
		return((string)$id);
	}
	
	/**
	 * Returns AS version supported by this backend.
	 *
	 * @return string
	 */
	public function GetSupportedASVersion()
	{
		return ZPush::ASV_14;
	}
	
	//
	// internally used functions
	//
	
	
	//
	// internally used utility functions
	//
	
	/**
	 * Create a b1gMail 'last updated' changelog entry.
	 *
	 * @param int $itemType Type of item
	 * @param int $itemID ID of item
	 * @param int $updated Timestamp
	 */
	private function ChangelogUpdated($itemType, $itemID, $updated)
	{
		$res = $this->db->Query('SELECT COUNT(*) FROM {pre}changelog WHERE `itemtype`=? AND `itemid`=?',
			$itemType, $itemID);
		list($count) = $res->FetchArray(MYSQL_NUM);
		$res->Free();

		if($count == 0)
		{
			$this->db->Query('INSERT INTO {pre}changelog(`itemtype`,`itemid`,`userid`,`updated`) VALUES(?,?,?,?)',
				$itemType, $itemID, $this->userID, $updated);
		}
		else
		{
			$this->db->Query('UPDATE {pre}changelog SET `updated`=? WHERE `itemtype`=? AND `itemid`=?',
				$updated, $itemType, $itemID);
		}
	}
	
	/**
	 * Create a b1gMail 'added' changelog entry.
	 *
	 * @param int $itemType Type of item
	 * @param int $itemID ID of item
	 * @param int $added Timestamp
	 */
	private function ChangelogAdded($itemType, $itemID, $added)
	{
		$res = $this->db->Query('SELECT COUNT(*) FROM {pre}changelog WHERE `itemtype`=? AND `itemid`=?',
			$itemType, $itemID);
		list($count) = $res->FetchArray(MYSQL_NUM);
		$res->Free();

		if($count == 0)
		{
			$this->db->Query('INSERT INTO {pre}changelog(`itemtype`,`itemid`,`userid`,`created`) VALUES(?,?,?,?)',
				$itemType, $itemID, $this->userID, $added);
		}
		else
		{
			$this->db->Query('UPDATE {pre}changelog SET `created`=? WHERE `itemtype`=? AND `itemid`=?',
				$added, $itemType, $itemID);
		}
	}
	
	/**
	 * Create a b1gMail 'deleted' changelog entry.
	 *
	 * @param int $itemType Type of item
	 * @param int $itemID ID of item
	 * @param int $deleted Timestamp
	 */
	private function ChangelogDeleted($itemType, $itemID, $deleted)
	{
		$res = $this->db->Query('SELECT COUNT(*) FROM {pre}changelog WHERE `itemtype`=? AND `itemid`=?',
			$itemType, $itemID);
		list($count) = $res->FetchArray(MYSQL_NUM);
		$res->Free();

		if($count == 0)
		{
			$this->db->Query('INSERT INTO {pre}changelog(`itemtype`,`itemid`,`userid`,`deleted`) VALUES(?,?,?,?)',
				$itemType, $itemID, $this->userID, $deleted);
		}
		else
		{
			$this->db->Query('UPDATE {pre}changelog SET `deleted`=? WHERE `itemtype`=? AND `itemid`=?',
				$deleted, $itemType, $itemID);
		}
	}
	
	/**
	 * Guess MIME type of a picture by picture file signature.
	 *
	 * @param string $data Image data (at least the first 4 bytes)
	 * @return string MIME type
	 */
	private function GuessPictureType($data)
	{
		if(substr($data, 0, 4) == "\x47\x49\x46\x38")
			return('image/gif');
		else if(substr($data, 0, 4) == "\xFF\xD8\xFF\xE0")
			return('image/jpg');
		else if(substr($data, 0, 4) == "\x89\x50\x4E\x47")
			return('image/png');
		else
			return('image/unknown');
	}
	
	/**
	 * Get file name of data item.
	 *
	 * @param int $id Item ID
	 * @param string $ext Extension
	 * @return string
	 */
	private function DataFilename($id, $ext = 'msg')
	{
		$dir = $this->prefs['datafolder'];

		if(file_exists($dir . $id . '.' . $ext))
			return($dir . $id . '.' . $ext);

		for($i=0; $i<strlen((string)$id); $i++)
		{
			$dir .= substr((string)$id, $i, 1);
			if(($i+1) % 2 == 0)
			{
				$dir .= '/';
				if(!file_exists($dir) && $this->prefs['structstorage'] == 'yes' && ($i<strlen((string)$id)-1))
				{
					@mkdir($dir, 0777);
					@chmod($dir, 0777);
				}
			}
		}

		if(substr($dir, -1) == '/')
			$dir = substr($dir, 0, -1);
		$dir .= '.' . $ext;

		if(file_exists($dir) || $this->prefs['structstorage'] == 'yes')
			return($dir);
		else 
			return($this->prefs['datafolder'] . $id . '.' . $ext);
	}
	
	/**
	 * Get b1gMail preferences.
	 *
	 * @return array
	 */
	private function GetPrefs()
	{
		$res = $this->db->Query('SELECT * FROM {pre}prefs LIMIT 1');
		$prefs = $res->FetchArray(MYSQL_ASSOC);
		$res->Free();
		
		return($prefs);
	}
	
	/**
	 * split string by $separator, taking care of "quotations"
	 *
	 * @param string $string Input
	 * @param mixed $separator Separator(s), may be an array
	 * @return array
	 */
	private function ExplodeOutsideOfQuotation($string, $separator, $preserveQuotes = false)
	{
		$result = array();

		$inEscape = $inQuote = false;
		$tmp = '';
		for($i=0; $i<strlen($string); $i++)
		{
			$c = $string[$i];
			if(((!is_array($separator) && $c == $separator)
				|| (is_array($separator) && in_array($c, $separator)))
				&& !$inQuote
				&& !$inEscape)
			{
				if(trim($tmp) != '')
				{
					$result[] = trim($tmp);
					$tmp = '';
				}
			}
			else if($c == '"' && !$inEscape)
			{
				$inQuote = !$inQuote;
				if($preserveQuotes)
					$tmp .= $c;
			}
			else if($c == '\\' && !$inEscape)
				$inEscape = true;
			else
			{
				$tmp .= $c;
				$inEscape = false;
			}
		}
		if(trim($tmp) != '')
		{
			$result[] = trim($tmp);
			$tmp = '';
		}

		return($result);
	}
	
	/**
	 * Extract text from parsed mail.
	 *
	 * @param object $parsedMail Parsed mail as returned by Mail_mimeDecode class
	 * @param string $type Text type (plain/html)
	 * @return string
	 */
	private function GetTextFromParsedMail($parsedMail, $type)
	{		
		$result = '';
		$objs = array($parsedMail);
		
		while(count($objs) > 0)
		{
			$obj = array_shift($objs);
			
			if(!isset($obj->ctype_primary))
				continue;
			
			if(strtolower($obj->ctype_primary) == 'text' && strtolower($obj->ctype_secondary) == strtolower($type))
				$result .= $obj->body;
			else if(strtolower($obj->ctype_primary) == 'multipart' && !empty($obj->parts))
				$objs = array_merge($objs, $obj->parts);
		}
		
		return($result);
	}
	
	/**
	 * Get attachment parts from mail.
	 *
	 * @param object $parsedMail Parsed mail as returned by Mail_mimeDecode class
	 * @return array
	 */
	private function GetAttachmentsFromParsedMail($parsedMail)
	{
		$result = array();

		if(!isset($parsedMail->parts))
			return($result);
		
		$objs = $parsedMail->parts;
		
		while(is_array($objs) && count($objs) > 0)
		{
			$part = array_shift($objs);
			
			if(!isset($part->ctype_primary))
				continue;
			
			// also process sub-parts
			if(strtolower($part->ctype_primary) == 'multipart' && in_array(strtolower($part->ctype_secondary), array('alternative', 'mixed', 'related')))
			{
				$objs = array_merge($objs, $part->parts);
				continue;
			}
			
			// do not consider text parts as attachment
			if(strtolower($part->ctype_primary) == 'text')
				continue;
			
			// if content disposition is set, only proceed if it is set to attachment or inline
			if(!empty($part->disposition) && !in_array($part->disposition, array('attachment', 'inline')))
				continue;
			
			$result[] = $part;
		}
		
		return($result);
	}

	/**
	 * update mail space
	 *
	 * @param int $bytes Bytes (negative or positive)
	 * @return boolean
	 */
	private function UpdateMailSpace($bytes)
	{
		if($bytes == 0)
			return(true);
		
		if($bytes < 0)
		{
			$this->db->Query('UPDATE {pre}users SET `mailspace_used`=GREATEST(0,`mailspace_used`-' . abs($bytes) . ') WHERE `id`=?',
				$this->userID);
			$this->userRow['mailspace_used'] -= abs($bytes);
		}
		else if($bytes > 0)
		{
			$this->db->Query('UPDATE {pre}users SET `mailspace_used`=GREATEST(0,`mailspace_used`+' . abs($bytes) . ') WHERE `id`=?',
				$this->userID);
			$this->userRow['mailspace_used'] += abs($bytes);
		}
		
		return(true);
	}

	/**
	 * Increase the account's mailbox generation.
	 */
	private function IncMailboxGeneration()
	{
		$this->db->Query('UPDATE {pre}users SET `mailbox_generation`=`mailbox_generation`+1 WHERE `id`=?',
			$this->userID);
	}
	
	/**
	 * Increment the acocunt's mailbox structure generation
	 */
	private function IncMailboxStructureGeneration()
	{
		$this->db->Query('UPDATE {pre}users SET `mailbox_structure_generation`=`mailbox_structure_generation`+1 WHERE `id`=?',
			$this->userID);
	}
	
	/**
	 * Set body of a sync object.
	 *
	 * @param SyncObject &$result Sync object
	 * @param string $text Text
	 */
	private function SetBody(&$result, $text)
	{
		if(Request::GetProtocolVersion() >= 12.0)
		{
			$result->asbody = new SyncBaseBody();
			$result->asbody->data 				= $text;
			$result->asbody->type 				= SYNC_BODYPREFERENCE_PLAIN;
			$result->asbody->truncated 			= false;
			$result->asbody->estimatedDataSize 	= strlen($result->asbody->data);
		}
		else
		{
			$result->body						= $text;
			$result->bodytruncated				= false;
			$result->bodysize					= strlen($result->body);
		}
	}
	
	/**
	 * Get body of a sync object.
	 *
	 * @param SyncObject $obj Sync object
	 * @return string
	 */
	private function GetBody($obj)
	{
		if(Request::GetProtocolVersion() >= 12.0 && isset($obj->asbody))
		{
			if($obj->asbody->type == SYNC_BODYPREFERENCE_HTML)
				return(Utils::ConvertHtmlToText($obj->asbody->data));
			else
				return($obj->asbody->data);
		}
		else
		{
			return($obj->body);
		}
		
		return('');
	}

	/**
	 * extract mail address from a string
	 *
	 * @param string $string
	 * @return string
	 */
	private function ExtractMailAddress($string)
	{
		$ret = '';
		$ret_arr = array();
		if(preg_match_all('/[a-zA-Z0-9&=\'\\.\\-_\\+]+@[a-zA-Z0-9.-]+\\.+[a-zA-Z]{2,6}/', $string, $ret_arr) > 0)
			$ret = $ret_arr[0][0];
		return($ret);
	}

	/**
	 * extract mail addresses from string
	 *
	 * @param string $string
	 * @return array
	 */
	private function ExtractMailAddresses($string)
	{
		$result = $ret_arr = array();
		preg_match_all('/[a-zA-Z0-9&=\'\\.\\-_\\+]+@[a-zA-Z0-9.-]+\\.+[a-zA-Z]{2,6}/', $string, $ret_arr);
		foreach($ret_arr[0] as $ret)
			if(!in_array($ret, $result))
				$result[] = $ret;
		return($result);
	}

	/**
	 * extract message ids from string
	 *
	 * @param string $str
	 * @return array
	 */
	private function ExtractMessageIDs($str)
	{
		$ret_arr = $result = array();
		preg_match_all('/<([^>]+)>/', $str, $ret_arr);
		foreach($ret_arr[0] as $ret)
			if(!in_array($ret, $result))
				$result[] = $ret;
		return($result);
	}

	/**
	 * get user's possible sender email addresses
	 * 
	 * @return array
	 */
	private function GetPossibleSenders()
	{
		$senders = array(strtolower($this->userRow['email']));

		// aliases
		$res = $this->db->Query('SELECT `email`,`type` FROM {pre}aliase WHERE `user`=?',
			$this->userID);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			if(($row['type'] & 1) != 0 && ($row['type'] & 4) == 0)
				$senders[] = strtolower($row['email']);
		}
		$res->Free();

		// workgroups
		$res = $this->db->Query('SELECT `email` FROM {pre}workgroups INNER JOIN {pre}workgroups_member ON {pre}workgroups.`id`={pre}workgroups_member.`workgroup` '
			. 'WHERE {pre}workgroups_member.`user`=?',
			$this->userID);
		while($row = $res->FetchArray(MYSQL_ASSOC))
		{
			$senders[] = strtolower($row['email']);
		}
		$res->Free();

		return($senders);
	}
};
