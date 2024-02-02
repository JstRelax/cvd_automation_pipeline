import smtplib
from email.mime.text import MIMEText

# SMTP server configuration
smtp_server = '192.168.1.74'  # Replace with the correct IP address of MailHog container
smtp_port = 1025            # Default MailHog SMTP port

# Email content
sender_email = "test@example.com"
receiver_email = "recipient@example.com"
subject = "Test Email"
body = "This is a test email sent from the SMTP server."

# Create a MIMEText object
message = MIMEText(body)
message['Subject'] = subject
message['From'] = sender_email
message['To'] = receiver_email

# Send the email
try:
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.send_message(message)
    print("Email sent successfully!")
except Exception as e:
    print(f"Error sending email: {e}")
