<?php
/*
 * Project         : b1gMail backend for Z-Push
 * File            : sendmail.php
 * Description     : Class for sending mail through SMTP/sendmail/mail().
 * Created         : 24.05.2013
 *
 * Copyright (C) 2013-2017 Patrick Schlangen <ps@b1g.de>
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

/**
 * Class for mail sending
 */
class b1gMailSendMail
{
	private $_recipients;
	private $_sender;
	private $_mailFrom;
	private $_subject;
	private $_fp;
	private $_userID;
	private $_prefs;

	/**
	 * constructor
	 *
	 * @return SendMail
	 */
	public function __construct($prefs)
	{
		$this->_recipients = array();
		$this->_sender = '';
		$this->_subject = '';
		$this->_mailFrom = false;
		$this->_userID = 0;
		$this->_prefs = $prefs;
	}

	/**
	 * set sender user ID
	 *
	 * @param int $userID
	 */
	public function SetUserID($userID)
	{
		$this->_userID = $userID;
	}

	/**
	 * set recipients
	 *
	 * @param mixed $recipients Recipient(s) (array or string)
	 */
	public function SetRecipients($recipients)
	{
		if(!is_array($recipients))
		{
			$this->_recipients = array($recipient);
		}
		else
		{
			$this->_recipients = array();
			foreach($recipients as $recipient)
				$this->_recipients[] = $recipient;
		}
	}

	/**
	 * set sender
	 *
	 * @param string $sender
	 */
	public function SetSender($sender)
	{
		$this->_sender = $sender;
	}

	/**
	 * set mail from address
	 *
	 * @param string $sender
	 */
	public function SetMailFrom($sender)
	{
		$this->_mailFrom = $sender;
	}

	/**
	 * set subject
	 *
	 * @param string $subject
	 */
	public function SetSubject($subject)
	{
		$this->_subject = $subject;
	}

	/**
	 * set body stream
	 *
	 * @param resource $fp
	 */
	public function SetBodyStream($fp)
	{
		$this->_fp = $fp;
	}

	/**
	 * send the mail
	 *
	 * @return bool
	 */
	public function Send()
	{
		if(count($this->_recipients) == 0)
			return(false);

		// send using mail()...
		if($this->_prefs['send_method'] == 'php')
			return($this->_sendUsingPHPMail());

		// ...or send using SMTP
		else if($this->_prefs['send_method'] == 'smtp')
			return($this->_sendUsingSMTP());

		// ...or send using sendmail
		else if($this->_prefs['send_method'] == 'sendmail')
			return($this->_sendUsingSendmail());
	}

	/**
	 * send mail using sendmail
	 *
	 * @return bool
	 */
	private function _sendUsingSendmail()
	{
		// build command
		$command = sprintf('%s -f "%s" %s',
			$this->_prefs['sendmail_path'],
			addslashes($this->_mailFrom !== false ? $this->_mailFrom : $this->_sender),
			addslashes(implode(' ', $this->_recipients)));

		// open
		$fp = popen($command, 'wb');
		if(!is_resource($fp))
		{
			ZLog::Write(sprintf('Failed to execute sendmail command <%s>',
				$command));
			return(false);
		}

		// send
		fseek($this->_fp, 0, SEEK_SET);
		while(!feof($this->_fp) && ($line = fgets($this->_fp)))
			fwrite($fp, rtrim($line, "\r\n") . "\n");

		// close
		return(pclose($fp) == 0);
	}

	/**
	 * send mail using PHP mail()
	 *
	 * @return bool
	 */
	private function _sendUsingPHPMail()
	{
		// line separators
		if(substr(PHP_OS, 0, 3) == 'WIN')
			$headerEOL = "\r\n";
		else
			$headerEOL = "\n";

		// get mail
		$messageHeader = $messageBody = '';
		$inBody = false;
		fseek($this->_fp, 0, SEEK_SET);
		while(!feof($this->_fp))
		{
			$line = rtrim(fgets($this->_fp), "\r\n");
			if(!$inBody && $line == '')
				$inBody = true;
			else
			{
				if($inBody)
					$messageBody .= $line . "\n";
				else if(substr($line, 0, 4) != 'To: '
					&& substr($line, 0, 9) != 'Subject: ')
					$messageHeader .= $line . $headerEOL;
			}
		}

		// send mail!
		if(ini_get('safe_mode'))
			$result = mail($this->_recipients[0],
				$this->_encodeMailHeaderField($this->_subject),
				$messageBody,
				$messageHeader);
		else
			$result = mail($this->_recipients[0],
				$this->_encodeMailHeaderField($this->_subject),
				$messageBody,
				$messageHeader,
				'-f "' . ($this->_mailFrom !== false ? $this->_mailFrom : $this->_sender) . '"');

		// return
		return($result);
	}

	/**
	 * send mail using SMTP
	 *
	 * @return bool
	 */
	private function _sendUsingSMTP()
	{
		// send using SMTP
		$smtp = new b1gMailSMTP($this->_prefs['smtp_host'], $this->_prefs['smtp_port'], $this->_prefs['b1gmta_host']);
		$smtp->SetUserID($this->_userID);
		if($smtp->Connect())
		{
			// login
			if($this->_prefs['smtp_auth'] == 'yes')
				$smtp->Login($this->_prefs['smtp_user'], $this->_prefs['smtp_pass']);

			// submit mail
			if($smtp->StartMail($this->_mailFrom !== false ? $this->_mailFrom : $this->_sender, $this->_recipients))
				$ok = $smtp->SendMail($this->_fp);
			else
				$ok = false;

			// disconnect
			$smtp->Disconnect();

			// return
			return($ok);
		}

		// return
		return(false);
	}

	/**
	 * encode mail header field
	 *
	 * @param string $text Text
	 * @return string
	 */
	private function _encodeMailHeaderField($text)
	{
		// replace line feeds and line breaks
		$text = str_replace(array("\r", "\n"), '', $text);

		// check if string is 8bit or contains non-printable characters
		$encode = $this->_shouldEncodeMailHeaderFieldText($text);

		// encode, if needed
		if($encode)
		{
			$fieldParts = array();
			$words = $this->_explodeOutsideOfQuotation($text, ' ', true);
			$i = 0;
			foreach($words as $word)
			{
				$encode = $this->_shouldEncodeMailHeaderFieldText($word);

				if(isset($fieldParts[$i]))
				{
					if($fieldParts[$i][0] == $encode)
						$fieldParts[$i][1] .= ' ' . $word;
					else
						$fieldParts[++$i] = array($encode, $word);
				}
				else
					$fieldParts[$i] = array($encode, $word);
			}

			$encodedText = '';
			foreach($fieldParts as $fieldPart)
			{
				if($fieldPart[0])
					$encodedText .= ' ' . sprintf('=?%s?B?%s?=',
						'UTF-8',
						base64_encode(trim($fieldPart[1])));
				else
					$encodedText .= ' ' . trim($fieldPart[1]);
			}

			return(trim($encodedText));
		}
		else
			return($text);
	}

	/**
	 * should $text be encoded?
	 *
	 * @param string $text Text
	 * @return bool
	 */
	private function _shouldEncodeMailHeaderFieldText($text)
	{
		for($i = 0; $i < strlen($text); $i++)
		{
			$dec = ord($text[ $i ]);
			if(($dec < 32) || ($dec > 126))
				return(true);
		}
		return(false);
	}

	/**
	 * split string by $separator, taking care of "quotations"
	 *
	 * @param string $string Input
	 * @param mixed $separator Separator(s), may be an array
	 * @return array
	 */
	private function _explodeOutsideOfQuotation($string, $separator, $preserveQuotes = false)
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
}

/**
 * Class for interaction with SMTP servers.
 */
class b1gMailSMTP
{
	private $_host;
	private $_port;
	private $_sock;
	private $_helo;
	private $_my_host;
	private $_isb1gMailServer;
	private $_userID;

	/**
	 * constructor
	 *
	 * @param string $host
	 * @param int $port
	 * @return BMSMTP
	 */
	public function __construct($host, $port, $my_host)
	{
		$this->_host = $host;
		$this->_port = $port;
		$this->_helo = false;
		$this->_my_host = $my_host;
		$this->_isb1gMailServer = false;
		$this->_userID = 0;
	}

	/**
	 * set sender user ID
	 *
	 * @param int $userID
	 */
	public function SetUserID($userID)
	{
		$this->_userID = $userID;
	}

	/**
	 * establish connection
	 *
	 * @return bool
	 */
	public function Connect()
	{
		$this->_sock = @fsockopen($this->_host, $this->_port, $errNo, $errStr);

		if(!is_resource($this->_sock))
		{
			ZLog::Write(LOGLEVEL_ERROR, sprintf('SMTP connection to <%s:%d> failed (%d, %s)',
				$this->_host,
				$this->_port,
				$errNo,
				$errStr));
			return(false);
		}
		else
		{
			$responseLine = $this->_getResponse();
			if(substr($responseLine, 0, 3) != '220')
			{
				ZLog::Write(LOGLEVEL_ERROR, sprintf('SMTP server <%s:%d> did not return +OK',
					$this->_host,
					$this->_port));
				return(false);
			}
			$this->_isb1gMailServer = strpos($responseLine, "[bMS-") !== false;
			return(true);
		}
	}

	/**
	 * log in
	 *
	 * @param string $user
	 * @param string $pass
	 * @return bool
	 */
	public function Login($user, $pass)
	{
		fwrite($this->_sock, 'EHLO ' . $this->_my_host . "\r\n")
			&& substr($this->_getResponse(), 0, 3) == '250'
			&& $this->_helo = true;

		if(fwrite($this->_sock, 'AUTH LOGIN' . "\r\n")
			&& substr($this->_getResponse(), 0, 3) == '334')
		{
			if(fwrite($this->_sock, base64_encode($user) . "\r\n")
				&& substr($this->_getResponse(), 0, 3) == '334')
			{
				if(fwrite($this->_sock, base64_encode($pass) . "\r\n")
					&& substr($this->_getResponse(), 0, 3) == '235')
				{
					return(true);
				}
				else
				{
					ZLog::Write(LOGLEVEL_ERROR, sprintf('SMTP server <%s:%d> rejected username or password for user <%s>',
						$this->_host,
						$this->_port,
						$user));
				}
			}
			else
			{
				ZLog::Write(sprintf('SMTP server <%s:%d> rejected username <%s>',
					$this->_host,
					$this->_port,
					$user));
			}
		}
		else
		{
			ZLog::Write(sprintf('SMTP server <%s:%d> does not seem to support LOGIN authentication',
				$this->_host,
				$this->_port,
				$user));
		}

		return(false);
	}

	/**
	 * disconnect
	 *
	 * @return bool
	 */
	public function Disconnect()
	{
		fwrite($this->_sock, 'QUIT' . "\r\n")
			&& $this->_getResponse();
		fclose($this->_sock);
		return(true);
	}

	/**
	 * initiate mail transfer
	 *
	 * @param string $from Sender address
	 * @param mixed $to Recipients (single address or array of addresses)
	 * @return bool
	 */
	public function StartMail($from, $to)
	{
		// send helo, if not sent yet (e.g. at login)
		if(!$this->_helo)
			fwrite($this->_sock, 'HELO ' . $this->_my_host . "\r\n")
				&& substr($this->_getResponse(), 0, 3) == '250'
				&& $this->_helo = true;

		// send MAIL FROM
		$mailFromCmd = 'MAIL FROM:<' . $from . '>';
		if($this->_isb1gMailServer)
		{
			$mailFromCmd .= ' X-B1GMAIL-USERID=' . $this->_userID;
		}
		$mailFromCmd .= "\r\n";
		if(fwrite($this->_sock, $mailFromCmd)
			&& substr($this->_getResponse(), 0, 3) == '250')
		{
			if(!is_array($to))
				$to = array($to);

			// send RCPT TO
			foreach($to as $address)
				fwrite($this->_sock, 'RCPT TO:<' . $address . '>' . "\r\n")
					&& $this->_getResponse();

			// ok!
			return(true);
		}
		else
		{
			ZLog::Write(sprintf('SMTP server <%s:%d> did not accept sender address <%s>',
				$this->_host,
				$this->_port,
				$from));
		}

		return(false);
	}

	/**
	 * send mail data
	 *
	 * @param resource $fp File pointer
	 * @return bool
	 */
	public function SendMail($fp)
	{
		// send DATA command
		if(fwrite($this->_sock, 'DATA' . "\r\n")
			&& substr($this->_getResponse(), 0, 3) == '354')
		{
			// send mail
			fseek($fp, 0, SEEK_SET);
			while(!feof($fp)
					&& ($line = fgets($fp)) !== false)
			{
				if(substr($line, 0, 1) == '.')
					$line = '.' . $line;

				if(fwrite($this->_sock, rtrim($line) . "\r\n") === false)
					break;
			}

			// finish
			return(fwrite($this->_sock, "\r\n" . '.' . "\r\n")
					&& substr($this->_getResponse(), 0, 3) == '250');
		}

		return(false);
	}

	/**
	 * reset session
	 *
	 * @return bool
	 */
	public function Reset()
	{
		return(fwrite($this->_sock, 'RSET' . "\r\n")
				&& substr($this->_getResponse(), 0, 3) == '250');
	}

	/**
	 * get smtp server response (may consist of multiple lines)
	 *
	 * @return string
	 */
	private function _getResponse()
	{
		$response = '';
		while($line = fgets($this->_sock))
		{
			$response .= $line;
			if($line[3] != '-')
				break;
		}
		return($response);
	}
}
