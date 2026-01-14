import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os

class EnviarEmail:
    def __init__(self, smtp_server, porta, email_remetente, senha):
        self.smtp_server = smtp_server
        self.porta = porta
        self.email_remetente = email_remetente
        self.senha = senha
    
    def enviar(self, assunto, df, destinatario, imagem=None, pdf=None):
        msg = MIMEMultipart()
        msg['From'] = self.email_remetente
        msg['To'] = destinatario
        msg['Subject'] = assunto
        
        # DataFrame como tabela HTML formatada no corpo
        html_table = f"""
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    background-color: #f4f4f4;
                    padding: 20px;
                }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    background-color: white;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                th {{
                    background-color: #4CAF50;
                    color: white;
                    padding: 12px;
                    text-align: left;
                    font-weight: bold;
                }}
                td {{
                    padding: 10px;
                    border-bottom: 1px solid #ddd;
                }}
                tr:hover {{
                    background-color: #f5f5f5;
                }}
                tr:nth-child(even) {{
                    background-color: #f9f9f9;
                }}
            </style>
        </head>
        <body>
            <h2 style="color: #333;">{assunto}</h2>
            {df.to_html(index=False, border=0)}
        </body>
        </html>
        """
        msg.attach(MIMEText(html_table, 'html'))
        
        # Anexar imagem se fornecida
        if imagem:
            with open(imagem, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename= {os.path.basename(imagem)}')
                msg.attach(part)
        
        # Anexar PDF se fornecido
        if pdf:
            with open(pdf, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename= {os.path.basename(pdf)}')
                msg.attach(part)
        
        # Enviar email
        server = smtplib.SMTP(self.smtp_server, self.porta)
        server.starttls()
        server.login(self.email_remetente, self.senha)
        server.send_message(msg)
        server.quit()
        print('Email enviado com sucesso!')