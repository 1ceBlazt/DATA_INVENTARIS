CREATE TABLE data_transaksi_penjualan(
    id_transaksi INTEGER,
    id_pelanggan INTEGER,
    nama_pelanggan VARCHAR(255),
    nama_produk VARCHAR(255),
    kategori_produk VARCHAR(255),
    jumlah INTEGER,
    hargaperunit INTEGER,
    tanggal_transaksi VARCHAR(255),
    metode_pembayaran VARCHAR(255)
);

select * from data_transaksi_penjualan dtp;