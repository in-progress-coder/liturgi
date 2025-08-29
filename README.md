# liturgi

Tool untuk membantu persiapan liturgi.

## Penggunaan
```
python liturgy_tool.py YYYY-MM-DD path/to/template.docx
```
Script akan membaca jadwal dari berkas `Jadwal Liturgi.xlsx` pada sheet `LITURGI INDUK`, menyalin berkas Word yang diberikan dan mengubah properti dokumen berdasarkan kolom pada baris dengan tanggal yang sesuai. Hasilnya disimpan dengan nama `Liturgi YYYY-MM-DD.docx`.

Mapping kolom Excel ke properti dokumen didefinisikan di dalam `liturgy_tool.py` sehingga dapat diperbarui atau dikembangkan lebih lanjut.
