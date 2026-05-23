# Que faire si l'ETL échoue à 06:00 UTC

## 1. Vérifier le statut du timer systemd

```bash
# Sur Hub EC2 (via SSM ou SSH WireGuard)
systemctl status etl-pipeline.timer
systemctl status etl-pipeline.service

# Voir les logs du dernier run
journalctl -u etl-pipeline.service -n 50 --no-pager
```

**Résultat attendu :** `Active: active (waiting)` pour le timer.
Si le service est en `failed`, noter le code d'erreur avant de continuer.

---

## 2. Relancer manuellement

```bash
# En tant qu'ubuntu sur le Hub EC2
cd /home/ubuntu/etl
source venv/bin/activate
python etl_pipeline.py
```

Le pipeline est **idempotent** : relancer ne créera pas de doublons
(contrainte UNIQUE sur `trade_id + source` et `alert_id`).

---

## 3. Vérifier la connectivité PostgreSQL

```bash
# Test connexion peer auth (ubuntu → trading_db)
psql -d trading_db -c "SELECT NOW();"

# Vérifier que les tables existent
psql -d trading_db -c "\dt raw.*"

# Nombre de lignes
psql -d trading_db -c "SELECT COUNT(*) FROM raw.trades;"
```

**Erreur courante :** `role "ubuntu" does not exist`
→ Recréer le rôle : `sudo -u postgres createuser ubuntu`

---

## 4. Vérifier l'accès S3

```bash
# Lister le bucket
aws s3 ls s3://cle-portfolio-etl/raw/trades/ --region us-west-1

# Test d'upload
echo "test" | aws s3 cp - s3://cle-portfolio-etl/test.txt --region us-west-1
aws s3 rm s3://cle-portfolio-etl/test.txt --region us-west-1
```

**Erreur courante :** `Unable to locate credentials`
→ Vérifier le rôle IAM attaché à l'instance EC2 (doit avoir `s3:PutObject` sur le bucket).

---

## 5. Vérifier le rsync Wazuh

```bash
# Sur Hub EC2 : vérifier si le fichier d'alertes est récent
ls -la /home/ubuntu/etl/wazuh_alerts.json
stat /home/ubuntu/etl/wazuh_alerts.json | grep Modify

# Sur Wazuh EC2 (10.66.66.3) : vérifier le timer rsync
systemctl status wazuh-rsync.timer
journalctl -u wazuh-rsync.service -n 20 --no-pager
```

**Erreur courante :** fichier vieux de plus de 20 minutes
→ Sur Wazuh EC2, relancer manuellement :
```bash
systemctl start wazuh-rsync.service
```

---

## 6. Erreurs courantes et correctifs

| Erreur | Cause | Correctif |
|--------|-------|-----------|
| `FileNotFoundError: wazuh_alerts.json` | Rsync Wazuh en retard | Vérifier timer sur Wazuh EC2 |
| `FATAL: role "root" does not exist` | Script lancé en root | Lancer en tant que `ubuntu` |
| `psycopg2.OperationalError: could not connect` | PostgreSQL arrêté | `sudo systemctl restart postgresql` |
| `NoCredentialsError` | Pas de rôle IAM EC2 | Vérifier IAM instance profile |
| `ParamValidationError` | Version boto3 incompatible | `pip install boto3 --upgrade` |
| `ModuleNotFoundError` | venv non activé | `source /home/ubuntu/etl/venv/bin/activate` |
| `UniqueViolation` | Impossible — idempotent | Ne devrait pas arriver (ON CONFLICT DO NOTHING) |
| Prefect server timeout | Port 8007 occupé | Tuer l'ancien processus : `pkill -f prefect` |

---

## 7. Réinitialiser le pipeline en cas de corruption

```bash
# ATTENTION : supprime toutes les données ingérées
sudo -u postgres psql -d trading_db -c "TRUNCATE raw.trades, raw.wazuh_alerts RESTART IDENTITY;"

# Puis relancer
python /home/ubuntu/etl/etl_pipeline.py
```

---

## Contacts et ressources

- **Hub EC2** : `i-0daa27a77ea6c24b0` (us-west-1) — accès via AWS SSM
- **Wazuh EC2** : `i-0ac7c3966a1530557` (10.66.66.3) — accès via WireGuard + SSH
- **Logs ETL** : `journalctl -u etl-pipeline.service`
- **Dashboard Metabase** : `http://10.66.66.1:3000/dashboard/2`
