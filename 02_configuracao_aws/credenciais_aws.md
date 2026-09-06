## 📋 Célula 1: Gerar Chaves de Acesso

### Localizar as Chaves

1. Tela AWS Academy'
2. Vá para a aba **"Laboratório de aprendizagem da AWS Academy"**
3. Inicie o Laboratório **"Start Lab"**
4. Clique em **"AWS Details"**
5. Clique em SHOW em: **"AWS CLI (Interface de linha de comando)"**

ATENÇÃO: As chaves de autenticação são atualizadas a cada 8 horas, portanto, você precisará gerar novas chaves de acesso periodicamente.


## 🖥️ Passo 2: Configurar no Seu Computador

**Opção 1: Instalar a extensão AWS no VsCode**

Extenções -> AWS Toolkit -> Instalar
Editar -> Abrir Paleta de Comandos -> AWS: Connect to AWS -> Criar perfil -> Colar as chaves de acesso
Como alternativa, você pode configurar as credenciais da AWS usando variáveis de ambiente ou o arquivo de configuração do AWS CLI.


**Opção 2: Variáveis de Ambiente**

Download and run the AWS CLI MSI installer for Windows (64-bit):
https://awscli.amazonaws.com/AWSCLIV2.msi

```powershell
aws configure

AWS Access Key ID [None]: AKIA...
AWS Secret Access Key [None]: wJal...
Default region name [None]: us-east-1
Default output format [None]: json
```
