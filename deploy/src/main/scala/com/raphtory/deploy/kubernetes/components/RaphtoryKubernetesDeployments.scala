package com.raphtory.deploy.kubernetes.components

import com.raphtory.deploy.kubernetes.utils.KubernetesDeployment

/** Extends KubernetesClient which extends Config.
  * KubernetesClient is used to establish kubernetes connection.
  * Kubernetes objects that are iterated over are read from application.conf values.
  *
  * @see  [[Config]]
  * [[KubernetesClient]]
  * [[KubernetesDeployment]]
  */

object RaphtoryKubernetesDeployments extends KubernetesClient {
  val systemEnvVars = System.getenv

  /** Create kubernetes deployments needed for Raphtory (if toggled in application.conf) */
  def create(): Unit =
    raphtoryKubernetesDeployments.forEach { raphtoryComponent =>
      if (
              conf.hasPath(s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.create") &&
              conf.getBoolean(s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.create")
      ) {
        val deploymentName: String                =
          s"raphtory-$raphtoryDeploymentId-$raphtoryComponent".toLowerCase()
        val deploymentLabels: Map[String, String] = Map(
                "deployment"         -> "raphtory",
                "raphtory/job"       -> s"$raphtoryDeploymentId",
                "raphtory/component" -> s"$raphtoryComponent"
        )

        var componentEnvVars = Map[String, String]()

        // Add deploy id and component name to map
        componentEnvVars += ("RAPHTORY_DEPLOY_ID"           -> s"$raphtoryDeploymentId")
        componentEnvVars += ("RAPHTORY_JAVA_COMPONENT_NAME" -> raphtoryComponent.toLowerCase())

        // Add in config defined env vars
        val configComponentsEnvVars =
          conf
            .getConfig(s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.pods.env")
            .root()
            .keySet()
        val configAllEnvVars        =
          conf.getConfig(s"raphtory.deploy.kubernetes.deployments.all.pods.env").root().keySet()

        configAllEnvVars.forEach { name =>
          componentEnvVars += (name -> conf.getString(
                  s"raphtory.deploy.kubernetes.deployments.all.pods.env.$name"
          ))
        }

        configComponentsEnvVars.forEach { name =>
          componentEnvVars += (name -> conf.getString(
                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.pods.env.$name"
          ))
        }

        // For each env var passed in, check if prefix matches and add to componentEnvVars map if it does
        val systemAllEnvVarsPrefix       = s"RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_"
        val systemComponentEnvVarsPrefix =
          s"RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_${raphtoryComponent.toUpperCase()}_PODS_ENV_"

        systemEnvVars.forEach { (name, value) =>
          name match {
            case name if name.startsWith(systemAllEnvVarsPrefix)       =>
              componentEnvVars += name.replaceAll(systemAllEnvVarsPrefix, "") -> value
            case name if name.startsWith(systemComponentEnvVarsPrefix) =>
              componentEnvVars += name.replaceAll(systemComponentEnvVarsPrefix, "") -> value
            case _                                                     =>
          }
        }

        try {
          raphtoryKubernetesLogger.info(
                  s"Deploying $deploymentName deployment for $raphtoryComponent component"
          )

          KubernetesDeployment.create(
                  client = kubernetesClient,
                  namespace = raphtoryKubernetesNamespaceName,
                  deploymentConfig = KubernetesDeployment.build(
                          name = deploymentName,
                          labels = deploymentLabels,
                          matchLabels = deploymentLabels,
                          containerName =
                            s"raphtory-$raphtoryDeploymentId-$raphtoryComponent".toLowerCase(),
                          containerImagePullPolicy = conf.getString(
                                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.pods.imagePullPolicy"
                          ),
                          imagePullSecretsName = raphtoryKubernetesDockerRegistrySecretName,
                          replicas = conf.getInt(
                                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.pods.replicas"
                          ),
                          containerImage = conf.getString(
                                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.pods.image"
                          ),
                          containerPort = conf.getInt(
                                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.pods.port"
                          ),
                          environmentVariables = componentEnvVars,
                          resources = conf.getConfig(
                                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.pods.resources"
                          )
                  )
          )
        }
        catch {
          case e: Throwable =>
            raphtoryKubernetesLogger.error(
                    s"Error found when deploying $deploymentName deployment for $raphtoryComponent component",
                    e
            )
        }
      }
      else
        raphtoryKubernetesLogger.info(
                s"Setting raphtory.deploy.kubernetes.deployments.$raphtoryComponent.create is set to false"
        )
    }

  /** Delete kubernetes deployments needed for Raphtory (if toggled in application.conf) */
  def delete(): Unit =
    raphtoryKubernetesDeployments.forEach { raphtoryComponent =>
      if (conf.hasPath(s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.create")) {
        val deploymentName: String =
          s"raphtory-$raphtoryDeploymentId-$raphtoryComponent".toLowerCase()
        val deployment             =
          try Option(
                  KubernetesDeployment.get(
                          client = kubernetesClient,
                          namespace = raphtoryKubernetesNamespaceName,
                          name = deploymentName
                  )
          )
          catch {
            case e: Throwable =>
              raphtoryKubernetesLogger.error(
                      s"Error found when getting $deploymentName deployment for $raphtoryComponent component",
                      e
              )
          }

        deployment match {
          case None        =>
            raphtoryKubernetesLogger.debug(
                    s"Deployment $deploymentName not found for $raphtoryComponent. Deployment delete aborted"
            )
          case Some(value) =>
            raphtoryKubernetesLogger.info(
                    s"Deployment $deploymentName found for $raphtoryComponent. Deleting deployment"
            )

            try KubernetesDeployment.delete(
                    client = kubernetesClient,
                    namespace = raphtoryKubernetesNamespaceName,
                    name = deploymentName
            )
            catch {
              case e: Throwable =>
                raphtoryKubernetesLogger.error(
                        s"Error found when deleting $deploymentName deployment for $raphtoryComponent component",
                        e
                )
            }
        }
      }
    }
}
