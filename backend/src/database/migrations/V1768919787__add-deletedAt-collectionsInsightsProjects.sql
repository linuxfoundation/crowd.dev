-- Migration to add deletedAt column to collectionsInsightsProjects table for soft delete functionality

ALTER TABLE public."collectionsInsightsProjects" 
ADD COLUMN "deletedAt" timestamp with time zone NULL DEFAULT NULL;
