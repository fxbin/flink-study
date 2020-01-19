CREATE TABLE `user` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `username` varchar(100) NOT NULL,
  `age` int(11) NOT NULL,
  `create_date` date DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


INSERT INTO `auth_center`.`user`(`id`, `username`, `age`, `create_date`) VALUES (3, 'DONE', 15, '2020-01-11');
INSERT INTO `auth_center`.`user`(`id`, `username`, `age`, `create_date`) VALUES (5, 'ECHO', 20, '2020-01-11');
INSERT INTO `auth_center`.`user`(`id`, `username`, `age`, `create_date`) VALUES (7, 'MARY', 55, '2020-01-11');
