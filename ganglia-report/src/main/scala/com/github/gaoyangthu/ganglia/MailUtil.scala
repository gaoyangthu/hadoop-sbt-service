package com.github.gaoyangthu.ganglia

/**
 * Created by GaoYang on 2014/5/21 0021.
 */

import org.apache.commons.mail._

object mail {
	implicit def stringToSeq(single: String): Seq[String] = Seq(single)
	implicit def liftToOption[T](t: T): Option[T] = Some(t)

	sealed abstract class MailType
	case object Plain extends MailType
	case object Rich extends MailType
	case object MultiPart extends MailType

	case class Mail(hostName: String,
					userName: String,
					password: String,
					from: (String, String), // (email -> name)
					to: Seq[String],
					cc: Seq[String] = Seq.empty,
					bcc: Seq[String] = Seq.empty,
					subject: String,
					message: String,
					richMessage: Option[String] = None,
					attachment: Option[(java.io.File)] = None)

	object send {
		def a(mail: Mail) {
			val format =
				if (mail.attachment.isDefined) MultiPart
				else if (mail.richMessage.isDefined) Rich
				else Plain

			val commonsMail: Email = format match {
				case Plain => new SimpleEmail().setMsg(mail.message)
				case Rich => new HtmlEmail().setHtmlMsg(mail.richMessage.get).setTextMsg(mail.message)
				case MultiPart => {
					val attachment = new EmailAttachment()
					attachment.setPath(mail.attachment.get.getAbsolutePath)
					attachment.setDisposition(EmailAttachment.ATTACHMENT)
					attachment.setName(mail.attachment.get.getName)
					new MultiPartEmail().attach(attachment).setMsg(mail.message)
				}
			}

			commonsMail.setHostName(mail.hostName)
			commonsMail.setAuthentication(mail.userName, mail.password)

			// Can't add these via fluent API because it produces exceptions
			mail.to foreach (commonsMail.addTo(_))
			mail.cc foreach (commonsMail.addCc(_))
			mail.bcc foreach (commonsMail.addBcc(_))

			commonsMail.
				setFrom(mail.from._1, mail.from._2).
				setSubject(mail.subject).
				send()
		}
	}
}