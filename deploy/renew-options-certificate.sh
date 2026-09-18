#!/bin/sh
set -eu

domain="hk.7894aa.cc.cd"
state_dir="/var/lib/rithmic-dtc-bridge"

install -o rithmic -g rithmic -m 0644 \
  "/etc/letsencrypt/live/${domain}/fullchain.pem" \
  "${state_dir}/public.crt"
install -o rithmic -g rithmic -m 0600 \
  "/etc/letsencrypt/live/${domain}/privkey.pem" \
  "${state_dir}/public.key"

systemctl restart rithmic-options-public.service
