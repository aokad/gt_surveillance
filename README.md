# gt_surveillance

download bam files from TCGA.

## Dependency

python >= 2.7

Python packages

 - drmaa

External script or binary.

 - gtdownload
 - xmlsplitter.pl

## install

```
git clone https://github.com/aokad/gt_surveillance.git
```

## Run

```
export DRMAA_LIBRARY_PATH=/geadmin/N1GE/lib/lx-amd64/libdrmaa.so.1.0
python {install path}/gt_surveillance.py {output root path} {key file} {manifest download from TCGA}
```

## Output directory

```
{output root path}
 |-- data                  # downloaded bam files
 |-- log                   # log files
 |-- manifests             # manifests splitted 1
 |-- scripts               # running scripts
```
