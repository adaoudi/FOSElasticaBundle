<?php

namespace FOS\ElasticaBundle\Doctrine;

use Doctrine\Common\Persistence\Event\LifecycleEventArgs;
use FOS\ElasticaBundle\Persister\ObjectPersister;
use FOS\ElasticaBundle\Persister\ObjectPersisterInterface;
use FOS\ElasticaBundle\Provider\IndexableInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\PropertyAccess\PropertyAccess;
use Symfony\Component\PropertyAccess\PropertyAccessorInterface;
use Monolog\Logger;
use Monolog\Handler\RotatingFileHandler;

/**
 * Automatically update ElasticSearch based on changes to the Doctrine source
 * data. One listener is generated for each Doctrine entity / ElasticSearch type.
 */
class Listener
{
    /**
     * Object persister.
     *
     * @var ObjectPersisterInterface
     */
    protected $objectPersister;

    /**
     * Configuration for the listener.
     *
     * @var array
     */
    private $config;

    /**
     * Objects scheduled for insertion.
     *
     * @var array
     */
    public $scheduledForInsertion = array();

    /**
     * Objects scheduled to be updated or removed.
     *
     * @var array
     */
    public $scheduledForUpdate = array();

    /**
     * IDs of objects scheduled for removal.
     *
     * @var array
     */
    public $scheduledForDeletion = array();

    /**
     * PropertyAccessor instance.
     *
     * @var PropertyAccessorInterface
     */
    protected $propertyAccessor;

    /**
     * @var IndexableInterface
     */
    private $indexable;

    /**
     * @var Logger
     */
    protected $logger;

    /**
     * Constructor.
     *
     * @param ObjectPersisterInterface $objectPersister
     * @param IndexableInterface       $indexable
     * @param array                    $config
     * @param LoggerInterface          $logger
     */
    public function __construct(
        ObjectPersisterInterface $objectPersister,
        IndexableInterface $indexable,
        array $config = array(),
        LoggerInterface $logger = null
    ) {
        $this->config = array_merge(array(
            'identifier' => 'id',
        ), $config);
        $this->indexable = $indexable;
        $this->objectPersister = $objectPersister;
        $this->propertyAccessor = PropertyAccess::createPropertyAccessor();

        $this->logger = new Logger('elasticSearch');
        $this->logger->pushHandler(new RotatingFileHandler($elasticSearchLogPath, Logger::CRITICAL));

        if ($logger && $this->objectPersister instanceof ObjectPersister) {
            $this->objectPersister->setLogger($logger);
        }
    }

    /**
     * Looks for new objects that should be indexed.
     *
     * @param LifecycleEventArgs $eventArgs
     */
    public function postPersist(LifecycleEventArgs $eventArgs)
    {
        $entity = $eventArgs->getObject();

        if ($this->objectPersister->handlesObject($entity) && $this->isObjectIndexable($entity)) {
            $this->scheduledForInsertion[] = $entity;
        }
    }

    /**
     * Looks for objects being updated that should be indexed or removed from the index.
     *
     * @param LifecycleEventArgs $eventArgs
     */
    public function postUpdate(LifecycleEventArgs $eventArgs)
    {
        $entity = $eventArgs->getObject();

        if ($this->objectPersister->handlesObject($entity)) {
            if ($this->isObjectIndexable($entity)) {
                $this->scheduledForUpdate[] = $entity;
            } else {
                // Delete if no longer indexable
                $this->scheduleForDeletion($entity);
            }
        }
    }

    /**
     * Delete objects preRemove instead of postRemove so that we have access to the id.  Because this is called
     * preRemove, first check that the entity is managed by Doctrine.
     *
     * @param LifecycleEventArgs $eventArgs
     */
    public function preRemove(LifecycleEventArgs $eventArgs)
    {
        $entity = $eventArgs->getObject();

        if ($this->objectPersister->handlesObject($entity)) {
            $this->scheduleForDeletion($entity);
        }
    }

    /**
     * Persist scheduled objects to ElasticSearch
     * After persisting, clear the scheduled queue to prevent multiple data updates when using multiple flush calls.
     */
    private function persistScheduled()
    {
        try {
            if (count($this->scheduledForInsertion)) {
                $this->objectPersister->insertMany($this->scheduledForInsertion);
                $this->scheduledForInsertion = array();
            }
        } catch (\Exception $e) {
            $this->logExceptionDetails($e, 'Insertion', $this->scheduledForInsertion);
        }

        try {
            if (count($this->scheduledForUpdate)) {
                $this->objectPersister->replaceMany($this->scheduledForUpdate);
                $this->scheduledForUpdate = array();
            }
        } catch (\Exception $e) {

            $this->logExceptionDetails($e, 'Updating', $this->scheduledForUpdate);
        }

        try {
            if (count($this->scheduledForDeletion)) {
                $this->objectPersister->deleteManyByIdentifiers($this->scheduledForDeletion);
                $this->scheduledForDeletion = array();
            }
        } catch (\Exception $e) {
            $this->logExceptionDetails($e, 'Deletion', $this->scheduledForDeletion);
        }
    }

    /**
     * @param \Exception $exception
     * @param string $action
     * @param array $scheduledEntities
     */
    private function logExceptionDetails(\Exception $exception, string $action, array $scheduledEntities ){
        $this->logger->error($exception->getMessage());
        foreach ($scheduledEntities as $entity){
            $entityName = 'unknown';
            $entityId = '?';
            if(is_object($entity) && method_exists($entity, 'getId')){
                $entityName = $this->objectPersister->getObjectClass();
                $entityId = $entity->getId();
            }else if(!is_object($entity) && is_int($entity)) {
                $entityName = $this->objectPersister->getObjectClass();
                $entityId = $entity;
            }

            $this->logger->critical($action.' entity: '.$entityName .' Id: '.$entityId);
        }
    }

    /**
     * Iterate through scheduled actions before flushing to emulate 2.x behavior.
     * Note that the ElasticSearch index will fall out of sync with the source
     * data in the event of a crash during flush.
     *
     * This method is only called in legacy configurations of the listener.
     *
     * @deprecated This method should only be called in applications that depend
     *             on the behaviour that entities are indexed regardless of if a
     *             flush is successful.
     */
    public function preFlush()
    {
        $this->persistScheduled();
    }

    /**
     * Iterating through scheduled actions *after* flushing ensures that the
     * ElasticSearch index will be affected only if the query is successful.
     */
    public function postFlush()
    {
        $this->persistScheduled();
    }

    /**
     * Record the specified identifier to delete. Do not need to entire object.
     *
     * @param object $object
     */
    private function scheduleForDeletion($object)
    {
        if ($identifierValue = $this->propertyAccessor->getValue($object, $this->config['identifier'])) {
            $this->scheduledForDeletion[] = !is_scalar($identifierValue) ? (string) $identifierValue : $identifierValue;
        }
    }

    /**
     * Checks if the object is indexable or not.
     *
     * @param object $object
     *
     * @return bool
     */
    private function isObjectIndexable($object)
    {
        return $this->indexable->isObjectIndexable(
            $this->config['indexName'],
            $this->config['typeName'],
            $object
        );
    }
}
