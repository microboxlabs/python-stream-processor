# Deployment

> TODO: Docker, Kubernetes, production setup

## Docker

```dockerfile
# TODO: Dockerfile example
```

## Kubernetes

```yaml
# TODO: K8s manifests
```

## Production Checklist

- [ ] Configure GCS storage
- [ ] Enable Redis for distributed state
- [ ] Set appropriate retention hours
- [ ] Configure Prometheus scraping
- [ ] Set up health checks

## Scaling

- Consumer uses Key_Shared subscription for horizontal scaling
- Multiple instances can process different devices in parallel
