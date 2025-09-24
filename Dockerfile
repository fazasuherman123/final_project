FROM apache/airflow:slim-3.0.2-python3.12

# USER root
# RUN apt-get update -qq && \
#     apt-get install -y wget curl nano apt-transport-https gnupg && \
#     wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /usr/share/keyrings/adoptium.gpg && \
#     echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" \
#     > /etc/apt/sources.list.d/adoptium.list && \
#     apt-get update -qq && \
#     apt-get install -y temurin-17-jdk && \
#     apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# 2. Kembali ke user airflow
USER airflow
ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME

# 3. Salin dan install requirements (untuk airflow dan dbt)
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# 4. (Opsional) Jika kamu ingin menambahkan project dbt ke dalam container
#     hanya jika kamu ingin menjalankan dbt dari BashOperator misalnya
#     atau ingin hardcode projectnya ke image ini
COPY final_project/ /usr/app/final_project/
WORKDIR /usr/app/final_project/

# 5. Install dbt dependencies
RUN dbt deps || echo "No dbt_project.yml found or dbt deps failed, skipping..."

# 6. Kembali ke working dir airflow agar airflow berjalan normal
WORKDIR $AIRFLOW_HOME
