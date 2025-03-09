DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- Создание таблиц
CREATE TABLE "Content" (
	"content_id" SERIAL NOT NULL UNIQUE,
	"type" VARCHAR(255) NOT NULL,
	"created_at" TIMESTAMP NOT NULL DEFAULT NOW(),
	"deleted_at" TIMESTAMP,
	PRIMARY KEY("content_id")
);

CREATE INDEX "Content_type_index"
ON "Content" ("type");


CREATE TABLE "Users" (
	"user_id" SERIAL NOT NULL UNIQUE,
	"username" VARCHAR(255) UNIQUE,
	"password_hash" VARCHAR(255),
	"email" VARCHAR(255) UNIQUE,
	"created_at" TIMESTAMP,
	PRIMARY KEY("user_id")
);


CREATE TABLE "Topics" (
	"content_id" SERIAL NOT NULL UNIQUE,
	"title" VARCHAR(255) NOT NULL UNIQUE,
	PRIMARY KEY("content_id")
);


CREATE TABLE "Messages" (
	"content_id" SERIAL NOT NULL UNIQUE,
	"topic_id" INTEGER NOT NULL,
	"message_content" TEXT NOT NULL,
	PRIMARY KEY("content_id")
);

CREATE INDEX "Messages_topic_id_index"
ON "Messages" ("topic_id");


CREATE TABLE "Logs" (
	"log_id" SERIAL NOT NULL UNIQUE,
	"user_id" INTEGER NOT NULL,
	"action_id" INTEGER NOT NULL,
	"is_success" BOOLEAN NOT NULL DEFAULT True,
	"created_at" TIMESTAMP NOT NULL,
	PRIMARY KEY("log_id")
);

CREATE INDEX "Logs_user_id_index"
ON "Logs" ("user_id");

CREATE INDEX "Logs_action_id_index"
ON "Logs" ("action_id");

CREATE INDEX "Logs_is_success_index"
ON "Logs" ("is_success");


CREATE TABLE "Actions" (
	"action_id" SERIAL NOT NULL UNIQUE,
	"type" VARCHAR(255) NOT NULL UNIQUE,
	PRIMARY KEY("action_id")
);


CREATE TABLE "Content_Log" (
	"id" SERIAL NOT NULL UNIQUE,
	"log_id" INTEGER NOT NULL UNIQUE,
	"content_id" INTEGER NOT NULL,
	PRIMARY KEY("id")
);

CREATE INDEX "Content_Log_index_0"
ON "Content_Log" ("content_id");


-- Добавление связей
ALTER TABLE "Logs"
ADD FOREIGN KEY("user_id") REFERENCES "Users"("user_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "Logs"
ADD FOREIGN KEY("action_id") REFERENCES "Actions"("action_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "Topics"
ADD FOREIGN KEY("content_id") REFERENCES "Content"("content_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "Messages"
ADD FOREIGN KEY("content_id") REFERENCES "Content"("content_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "Messages"
ADD FOREIGN KEY("topic_id") REFERENCES "Content"("content_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "Content_Log"
ADD FOREIGN KEY("log_id") REFERENCES "Logs"("log_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "Content_Log"
ADD FOREIGN KEY("content_id") REFERENCES "Content"("content_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;